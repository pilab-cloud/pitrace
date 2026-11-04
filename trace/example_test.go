package trace

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// ExampleNewBuilder shows the fluent construction of a recorder and service.
func ExampleNewBuilder() {
	rec, svc, err := NewBuilder().
		WithDir("/tmp/pitrace").
		WithRedactUploads(true).
		WithRedactDownloads(true).
		BuildService()
	if err != nil {
		fmt.Println("build:", err)
		return
	}
	defer rec.Close(context.Background())
	fmt.Println("recorder enabled:", rec.Enabled())
	fmt.Println("service:", svc != nil)
	// Output:
	// recorder enabled: true
	// service: true
}

// ExampleRecorder_Tail shows subscribing to live trace summaries.
func ExampleRecorder_Tail() {
	rec, err := NewRecorder(Config{Enabled: true, Dir: "/tmp/pitrace", QueueSize: 16}, zerolog.Nop())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rec.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop := rec.Tail(ctx, "", "", 1)
	defer stop()

	select {
	case s := <-ch:
		fmt.Println("got summary:", s.GetTraceId() != "")
	case <-time.After(time.Second):
		fmt.Println("no backlog")
	}
	// Output:
	// no backlog
}
