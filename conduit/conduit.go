package conduit

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

type Transform func(Transformable) Upload

type Conduit struct {
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
	DEFAULT_BATCH_SIZE         = 10
	DEFAULT_POLL_FREQUENCY     = 3000
	DEFAULT_VISIBILITY_TIMEOUT = 10
)

func NewConduit(s session.Session, f Transform) Conduit {
	return NewConduitWithConfig(s, f, &Config{})
}

func NewConduitWithConfig(s session.Session, f Transform, config *Config) Conduit {
	// Use defaults if config values were not set

	config.S3Egress = setDefaultString(config.S3Egress, aws.String(os.Getenv("CONDUIT_S3_EGRESS_BUCKET")), nil)
	config.QueueUrl = setDefaultString(config.QueueUrl, aws.String(os.Getenv("CONDUIT_QUEUE_URL")), nil)
	config.BatchSize = setDefaultInt64(config.BatchSize, aws.String(os.Getenv("CONDUIT_BATCH_SIZE")), aws.Int64(DEFAULT_BATCH_SIZE))
	config.PollFrequency = setDefaultInt64(config.PollFrequency, aws.String(os.Getenv("CONDUIT_POLL_FREQUENCY")), aws.Int64(DEFAULT_POLL_FREQUENCY))
	config.VisibilityTimeout = setDefaultInt64(config.VisibilityTimeout, aws.String(os.Getenv("CONDUIT_VISIBILITY_TIMEOUT")), aws.Int64(DEFAULT_VISIBILITY_TIMEOUT))

	return Conduit{
		Session:   s,
		Transform: f,
		Config:    *config,
	}
}

func (c Conduit) Delete(record Record) {
	svc := sqs.New(&c.Session)
	fmt.Println("Deleting record...")
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      c.QueueUrl,
		ReceiptHandle: aws.String(record.ReceiptHandle),
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Deleted record!")
}

func (c Conduit) Extract(record Record, ch chan<- Transformable) {
	fmt.Println("Extracting record...")
	downloader := s3manager.NewDownloader(&c.Session)
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

func (c Conduit) Load(upload Upload, ch chan<- Record) {
	fmt.Println("Loading record...")
	uploader := s3manager.NewUploader(&c.Session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: c.S3Egress,
		Key:    aws.String(upload.Key),
		Body:   strings.NewReader(upload.Data),
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	ch <- upload.Record
	fmt.Println("Loaded record!")
}

func (c Conduit) Receive(ch chan<- Record) {
	var response Response
	svc := sqs.New(&c.Session)
	fmt.Println("Polling for messages...")
	messages, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            c.QueueUrl,
		MaxNumberOfMessages: c.BatchSize,
		VisibilityTimeout:   c.VisibilityTimeout,
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

func (c Conduit) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(*c.PollFrequency) * time.Millisecond)
	extractQueue := make(chan Record)
	transformQueue := make(chan Transformable)
	loadQueue := make(chan Upload)
	deleteQueue := make(chan Record)

	go c.Receive(extractQueue)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			go c.Receive(extractQueue)
		case record := <-extractQueue:
			go c.Extract(record, transformQueue)
		case data := <-transformQueue:
			// Abstract away async channel internals from the implementer
			go func(data Transformable, queue chan<- Upload) {
				queue <- c.Transform(data)
			}(data, loadQueue)
		case upload := <-loadQueue:
			go c.Load(upload, deleteQueue)
		case record := <-deleteQueue:
			go c.Delete(record)
		}
	}
}

func setDefaultString(value *string, envValue *string, defaultValue *string) *string {
	// Config Heirarchy:
	//   Use config variables
	//   Use environment variables
	//   Use default values
	if value != nil {
		return value
	}
	if envValue != nil && *envValue != "" {
		return envValue
	}
	if defaultValue != nil {
		return defaultValue
	}
	panic("Required environment variable not defined.")
}

func setDefaultInt64(value *int64, env *string, defaultValue *int64) *int64 {
	// Use config variables
	// Use environment variables
	// Use default values
	envValue, err := strconv.ParseInt(*env, 10, 64)
	if value != nil {
		return value
	}
	if err == nil && *env != "" {
		return &envValue
	}
	if defaultValue != nil {
		return defaultValue
	}
	panic("Required environment variable not defined.")
}
