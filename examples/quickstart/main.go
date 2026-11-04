package main

import (
	"context"
	"fmt"
	"log"

	"go.pilab.hu/cloud/pitrace/trace"
)

func main() {
	rec, svc, err := trace.NewBuilder().
		WithDir("/tmp/pitrace").
		BuildService()
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Close(context.Background())

	fmt.Println("recorder enabled:", rec.Enabled())
	fmt.Println("service ready:", svc != nil)
}
