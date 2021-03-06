package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jrcasso/conduit/conduit"
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
	c := conduit.NewConduit(*sess, myTransform)

	c.Run(ctx)
}

func myTransform(t conduit.Transformable) conduit.Upload {
	fmt.Println("Transforming record...")
	// Do some transformation on t.Data
	t.Data = fmt.Sprintf("new data %v", t.Data)

	fmt.Println("Transformed record!")
	return conduit.Upload{
		Key:           fmt.Sprintf("transformed-%v", t.Record.S3.Object.Key),
		Transformable: t,
	}
}
