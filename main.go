package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jrcasso/transformer/transform"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel() }()
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String("us-east-1"),
			Endpoint:         aws.String("http://localstack:4566"),
		},
	}))
	t := transform.NewTransformer(*sess, myTransform, transform.Config{
		S3Ingress: os.Getenv("TRANSFORM_S3_INGRESS_BUCKET"),
		S3Egress:  os.Getenv("TRANSFORM_S3_EGRESS_BUCKET"),
		QueueUrl:  os.Getenv("TRANSFORM_QUEUE_URL"),
	})

	if err := t.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func myTransform(t transform.Transformable, uploadQueue chan<- transform.Upload) {
	fmt.Println("Transforming record...")
	// Do some transformation on t.Data
	newData := fmt.Sprintf("new data %v", t.Data)

	uploadQueue <- transform.Upload{
		Transformable: transform.Transformable{
			Data:   newData,
			Record: t.Record,
		},
		Key: fmt.Sprintf("transformed-%v", t.Record.S3.Object.Key),
	}
	fmt.Println("Transformed record!")
}
