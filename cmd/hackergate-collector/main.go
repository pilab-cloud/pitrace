// Command hackergate-collector is the stateless ingest tier of the hackergate
// pipeline. Sidecars stream exchanges to it over gRPC; it writes the bodies to
// MinIO/S3 as they arrive and the metadata, headers and search fields to
// MongoDB. It keeps no local state, so scale it horizontally behind any gRPC
// load balancer (an HPA on CPU, or a headless service plus client-side
// balancing, both work).
//
//	MONGODB_URI=mongodb://mongo:27017 \
//	MINIO_ENDPOINT=minio:9000 MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... \
//	hackergate-collector -listen :9470 -bucket hackergate
//
// -memory runs without any external dependency (development only).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.pilab.hu/cloud/pitrace/collector"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hackergate-collector:", err)
		os.Exit(1)
	}
}

type pinger interface{ Ping(context.Context) error }

func run() error {
	var (
		listen    = flag.String("listen", ":9470", "gRPC listen address")
		certFile  = flag.String("cert", "", "server certificate (PEM); empty serves plaintext (in-cluster mesh/dev)")
		keyFile   = flag.String("key", "", "server private key (PEM)")
		memory    = flag.Bool("memory", false, "keep everything in memory (development only)")
		mongoURI  = flag.String("mongo-uri", os.Getenv("MONGODB_URI"), "MongoDB connection string")
		mongoDB   = flag.String("mongo-db", envOr("MONGODB_DATABASE", "hackergate"), "MongoDB database")
		mongoColl = flag.String("mongo-coll", "exchanges", "MongoDB collection")
		endpoint  = flag.String("minio-endpoint", os.Getenv("MINIO_ENDPOINT"), "MinIO/S3 endpoint host:port")
		bucket    = flag.String("bucket", envOr("MINIO_BUCKET", "hackergate"), "bucket for bodies")
		useSSL    = flag.Bool("minio-ssl", false, "use TLS to MinIO/S3")
		retention = flag.Duration("retention", 7*24*time.Hour, "how long metadata and bodies are kept (0 = forever)")
		maxBody   = flag.Int64("max-body-bytes", collector.DefaultMaxBodyBytes, "largest accepted body")
		maxStream = flag.Int64("max-stream-bytes", collector.DefaultMaxStreamBytes, "largest accepted streamed body")
		debug     = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	level := zerolog.InfoLevel
	if *debug {
		level = zerolog.DebugLevel
	}
	logger := zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		meta   collector.MetaStore
		bodies collector.BodyStore
	)
	if *memory {
		meta, bodies = collector.NewMemoryMeta(), collector.NewMemoryBodies()
		logger.Warn().Msg("running with in-memory stores; nothing is persisted")
	} else {
		if *mongoURI == "" || *endpoint == "" {
			return fmt.Errorf("-mongo-uri (MONGODB_URI) and -minio-endpoint (MINIO_ENDPOINT) are required, or use -memory")
		}
		connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		m, err := collector.NewMongoMeta(connectCtx, collector.MongoConfig{
			URI: *mongoURI, Database: *mongoDB, Collection: *mongoColl, Retention: *retention,
		})
		if err != nil {
			return err
		}
		b, err := collector.NewMinioBodies(connectCtx, collector.MinioConfig{
			Endpoint:  *endpoint,
			AccessKey: os.Getenv("MINIO_ACCESS_KEY"),
			SecretKey: os.Getenv("MINIO_SECRET_KEY"),
			Bucket:    *bucket,
			UseSSL:    *useSSL,
			Retention: *retention,
		})
		if err != nil {
			return err
		}
		meta, bodies = m, b
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = meta.Close(c)
	}()

	var opts []grpc.ServerOption
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return fmt.Errorf("load certificate: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	} else {
		logger.Warn().Msg("serving plaintext gRPC; terminate TLS in the mesh or pass -cert/-key")
	}
	gs := grpc.NewServer(opts...)
	collector.Register(gs, meta, bodies, collector.NewIngest(meta, bodies, logger, collector.WithMaxBodyBytes(*maxBody), collector.WithMaxStreamBytes(*maxStream)))

	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	go watchHealth(ctx, hs, meta, bodies)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		hs.Shutdown()
		gs.GracefulStop()
	}()
	logger.Info().Str("addr", *listen).Msg("collector listening")
	return gs.Serve(ln)
}

// watchHealth reports NOT_SERVING while a backend is unreachable so the load
// balancer stops routing pushes to a replica that cannot store them.
func watchHealth(ctx context.Context, hs *health.Server, stores ...any) {
	check := func() {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		st := healthpb.HealthCheckResponse_SERVING
		for _, s := range stores {
			if p, ok := s.(pinger); ok && p.Ping(c) != nil {
				st = healthpb.HealthCheckResponse_NOT_SERVING
			}
		}
		hs.SetServingStatus("", st)
	}
	check()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			check()
		}
	}
}
