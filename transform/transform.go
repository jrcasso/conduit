package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// {
// 	"Records":[
// 		 {
// 				"eventVersion":"2.1",
// 				"eventSource":"aws:s3",
// 				"awsRegion":"us-east-1",
// 				"eventTime":"2021-10-03T05:05:03.622Z",
// 				"eventName":"ObjectCreated:Put",
// 				"userIdentity":{  // Not implemented
// 					 "principalId":"AIDAJDPLRKLG7UEXAMPLE"
// 				},
// 				"requestParameters":{  // Not implemented
// 					 "sourceIPAddress":"127.0.0.1"
// 				},
// 				"responseElements":{  // Not implemented
// 					 "x-amz-request-id":"27e06596",
// 					 "x-amz-id-2":"eftixk72aD6Ap51TnqcoF8eFidJG9Z/2"
// 				},
// 				"s3":{
// 					 "s3SchemaVersion":"1.0",  // Not implemented
// 					 "configurationId":"testConfigRule",  // Not implemented
// 					 "bucket":{
// 							"name":"ingress",
// 							"ownerIdentity":{  // Not implemented
// 								 "principalId":"A3NL1KOZZKExample"
// 							},
// 							"arn":"arn:aws:s3:::ingress"
// 					 },
// 					 "object":{
// 							"key":"Makefile",
// 							"size":59,
// 							"eTag":"2d21a73d66fe9a154b3e7e1442e82c1c",
// 							"versionId":null,
// 							"sequencer":"0055AED6DCD90281E5"
// 					 }
// 				}
// 		 }
// 	]
// }

type Transform func(Transformable, chan<- Upload)

type Transformer struct {
	Config
	Session   session.Session
	Transform Transform
}

type Config struct {
	BatchSize         *int64
	PollFrequency     *int64
	QueueUrl          *string
	S3Egress          *string
	VisibilityTimeout *int64
}

type Response struct {
	Records []Record `json:"Records"`
}

type Record struct {
	EventVersion  string `json:"eventVersion"`
	EventSource   string `json:"eventSource"`
	EwsRegion     string `json:"awsRegion"`
	EventTime     string `json:"eventTime"`
	EventName     string `json:"eventName"`
	ReceiptHandle string
	S3            S3Record `json:"s3"`
}

type S3Record struct {
	Bucket S3BucketRecord `json:"bucket"`
	Object S3ObjectRecord `json:"object"`
}

type S3ObjectRecord struct {
	ETag      string `json:"eTag"`
	Key       string `json:"key"`
	Size      int    `json:"size"`
	Sequencer string `json:"sequencer"`
}

type S3BucketRecord struct {
	Name string
	Arn  string
}

type Transformable struct {
	Record Record
	Data   string
}

type Upload struct {
	Key string
	Transformable
}

const (
	DEFAULT_BATCH_SIZE         = 1
	DEFAULT_POLL_FREQUENCY     = 5
	DEFAULT_VISIBILITY_TIMEOUT = 10
)

func NewTransformer(s session.Session, f Transform) Transformer {
	return NewTransformerWithConfig(s, f, &Config{})
}

func NewTransformerWithConfig(s session.Session, f Transform, config *Config) Transformer {
	// Use defaults if config values were not set
	batchSize, _ := strconv.ParseInt(os.Getenv("TRANSFORM_BATCH_SIZE"), 10, 64)
	pollFrequency, _ := strconv.ParseInt(os.Getenv("TRANSFORM_POLL_FREQUENCY"), 10, 64)
	visibilityTimeout, _ := strconv.ParseInt(os.Getenv("TRANSFORM_VISIBILITY_TIMEOUT"), 10, 64)

	setDefaultString(config.S3Egress, aws.String(os.Getenv("TRANSFORM_S3_EGRESS_BUCKET")), nil)
	setDefaultString(config.QueueUrl, aws.String(os.Getenv("TRANSFORM_QUEUE_URL")), nil)
	setDefaultInt64(config.BatchSize, aws.Int64(batchSize), aws.Int64(1))
	setDefaultInt64(config.PollFrequency, aws.Int64(pollFrequency), aws.Int64(3))
	setDefaultInt64(config.VisibilityTimeout, aws.Int64(visibilityTimeout), aws.Int64(10))

	return Transformer{
		Session:   s,
		Transform: f,
		Config:    *config,
	}
}

func (t Transformer) Delete(record Record) {
	svc := sqs.New(&t.Session)
	fmt.Println("Deleting record...")
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      t.QueueUrl,
		ReceiptHandle: aws.String(record.ReceiptHandle),
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Deleted record!")
}

func (t Transformer) Extract(record Record, ch chan<- Transformable) {
	fmt.Println("Extracting record...")
	downloader := s3manager.NewDownloader(&t.Session)
	buff := &aws.WriteAtBuffer{}
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: &record.S3.Bucket.Name,
		Key:    &record.S3.Object.Key,
	})

	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Extracted record!")
	ch <- Transformable{
		Record: record,
		Data:   string(buff.Bytes()),
	}
}

func (t Transformer) Load(upload Upload, ch chan<- Record) {
	fmt.Println("Loading record...")
	uploader := s3manager.NewUploader(&t.Session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: t.S3Egress,
		Key:    aws.String(upload.Key),
		Body:   strings.NewReader(upload.Data),
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	ch <- upload.Record
	fmt.Println("Loaded record!")
}

func (t Transformer) Receive(ch chan<- Record) {
	var response Response
	svc := sqs.New(&t.Session)
	fmt.Println("Polling for messages...")
	messages, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            t.QueueUrl,
		MaxNumberOfMessages: t.BatchSize,
		VisibilityTimeout:   t.VisibilityTimeout,
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	for _, message := range messages.Messages {
		json.Unmarshal([]byte(*message.Body), &response)
		if len(response.Records) == 1 {
			fmt.Println("Received message with records")
			// There should only ever be a single record
			response.Records[0].ReceiptHandle = *message.ReceiptHandle
			fmt.Println("Adding record to queue...")
			ch <- response.Records[0]
			fmt.Println("Added record to queue")
		} else if len(response.Records) > 1 {
			panic("Received more than one record from SQS!")
		} else {
			fmt.Println("No messages received.")
		}
	}
}

func (t Transformer) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(*t.PollFrequency) * time.Second)
	extractQueue := make(chan Record)
	transformQueue := make(chan Transformable)
	loadQueue := make(chan Upload)
	deleteQueue := make(chan Record)

	go t.Receive(extractQueue)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			go t.Receive(extractQueue)
		case record := <-extractQueue:
			go t.Extract(record, transformQueue)
		case data := <-transformQueue:
			go t.Transform(data, loadQueue)
		case upload := <-loadQueue:
			go t.Load(upload, deleteQueue)
		case record := <-deleteQueue:
			go t.Delete(record)
		}
	}
}

func setDefaultString(value *string, envValue *string, defaultValue *string) {
	// Use config variables
	// Use environment variables
	// Use default values
	if value != nil {
		return
	}
	if envValue != nil {
		*value = *envValue
		return
	}
	if defaultValue != nil {
		*value = *defaultValue
		return
	}
	panic("Required environment variable not defined.")
}

func setDefaultInt64(value *int64, envValue *int64, defaultValue *int64) {
	// Use config variables
	// Use environment variables
	// Use default values
	if value != nil {
		return
	}
	if envValue != nil {
		*value = *envValue
		return
	}
	if defaultValue != nil {
		*value = *defaultValue
		return
	}
	panic("Required environment variable not defined.")
}
