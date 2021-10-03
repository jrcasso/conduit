package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jrcasso/transformer/transform"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	visibility, _ := strconv.ParseInt(os.Getenv("TRANSFORM_VISIBILITY_TIMEOUT"), 10, 64)
	batchSize, _ := strconv.ParseInt(os.Getenv("TRANSFORM_BATCH_SIZE"), 10, 64)
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String("us-east-1"),
			Endpoint:         aws.String("http://localstack:4566"),
		},
		SharedConfigState: session.SharedConfigEnable,
	}))
	t := transform.Transformer{
		S3Ingress:  os.Getenv("TRANSFORM_S3_INGRESS_BUCKET"),
		S3Egress:   os.Getenv("TRANSFORM_S3_EGRESS_BUCKET"),
		QueueUrl:   os.Getenv("TRANSFORM_QUEUE_URL"),
		Visibility: visibility,
		BatchSize:  batchSize,
		Session:    *sess,
		Transform: func(t transform.Transformable, c chan<- transform.Upload) {
			fmt.Println("Transforming record...")
			c <- transform.Upload{
				Data: fmt.Sprintf("FOO %v", t.Data),
				Key:  "test-2",
			}
			fmt.Println("Transformed record!")
		},
	}

	defer func() {
		cancel()
	}()

	if err := run(ctx, t); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, t transform.Transformer) error {
	downloadQueue := make(chan transform.Record)
	transformQueue := make(chan transform.Transformable)
	uploadQueue := make(chan transform.Upload)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.Tick(5 * time.Second):
			go t.Receive(downloadQueue)
		case record := <-downloadQueue:
			go t.Download(record, transformQueue)
		case data := <-transformQueue:
			go t.Transform(data, uploadQueue)
		case upload := <-uploadQueue:
			go t.Upload(upload)
		}
	}
}
