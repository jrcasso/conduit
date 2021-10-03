package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

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
		Transform:  myTransform,
	}

	defer func() {
		cancel()
	}()

	if err := t.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func myTransform(t transform.Transformable, uploadQueue chan<- transform.Upload) {
	fmt.Println("Transforming record...")
	uploadQueue <- transform.Upload{
		Data: fmt.Sprintf("FOO %v", t.Data),
		Key:  "test-2",
	}
	fmt.Println("Transformed record!")
}
