package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"

	"go.pilab.hu/cloud/pitrace/trace"
)

// MinioConfig configures MinioBodies.
type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	// Retention expires bodies through a lifecycle rule on the bodies/ prefix
	// (rounded up to whole days); 0 keeps them forever. Setting it replaces
	// the bucket's lifecycle configuration, so use a dedicated bucket.
	Retention time.Duration
}

// MinioBodies is the S3/MinIO BodyStore.
type MinioBodies struct {
	client *minio.Client
	bucket string
}

// NewMinioBodies connects, creates the bucket when missing and applies the
// retention rule.
func NewMinioBodies(ctx context.Context, cfg MinioConfig) (*MinioBodies, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("collector: minio endpoint and bucket required")
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("collector: minio client: %w", err)
	}
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("collector: minio bucket check: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			// Another replica may have created it concurrently.
			if ok, _ := client.BucketExists(ctx, cfg.Bucket); !ok {
				return nil, fmt.Errorf("collector: minio make bucket: %w", err)
			}
		}
	}
	if cfg.Retention > 0 {
		days := int(math.Ceil(cfg.Retention.Hours() / 24))
		rule := lifecycle.Rule{
			ID:         "hackergate-bodies-retention",
			Status:     "Enabled",
			RuleFilter: lifecycle.Filter{Prefix: "bodies/"},
			Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(days)},
		}
		cfgLC := lifecycle.NewConfiguration()
		cfgLC.Rules = []lifecycle.Rule{rule}
		if err := client.SetBucketLifecycle(ctx, cfg.Bucket, cfgLC); err != nil {
			return nil, fmt.Errorf("collector: minio lifecycle: %w", err)
		}
	}
	return &MinioBodies{client: client, bucket: cfg.Bucket}, nil
}

// streamPartSize is the multipart part size for unknown-length uploads: it is
// also the memory held per concurrent upload.
const streamPartSize = 8 << 20

func (m *MinioBodies) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	if size < 0 {
		opts.PartSize = streamPartSize
	}
	_, err := m.client.PutObject(ctx, m.bucket, key, r, size, opts)
	return err
}

func (m *MinioBodies) Stat(ctx context.Context, key string) (int64, error) {
	info, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return 0, trace.ErrTraceNotFound
		}
		return 0, err
	}
	return info.Size, nil
}

func (m *MinioBodies) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := m.client.GetObject(ctx, m.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; Stat surfaces a missing key now rather than mid-stream.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, trace.ErrTraceNotFound
		}
		return nil, err
	}
	return obj, nil
}

func (m *MinioBodies) Delete(ctx context.Context, key string) error {
	return m.client.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{})
}

// Ping checks the bucket is reachable (readiness probes).
func (m *MinioBodies) Ping(ctx context.Context) error {
	_, err := m.client.BucketExists(ctx, m.bucket)
	return err
}
