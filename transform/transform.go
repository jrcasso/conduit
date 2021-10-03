package transform

import (
	"encoding/json"
	"fmt"
	"strings"

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

type Transformer struct {
	S3Ingress  string
	S3Egress   string
	QueueUrl   string
	Visibility int64
	BatchSize  int64
	Session    session.Session
	Transform  func(Transformable, chan<- Upload)
}

type Config struct {
	S3Ingress  string
	S3Egress   string
	QueueUrl   string
	Visibility int64
	BatchSize  int64
}

type Response struct {
	Records []Record `json:"Records"`
}

type Record struct {
	EventVersion string   `json:"eventVersion"`
	EventSource  string   `json:"eventSource"`
	EwsRegion    string   `json:"awsRegion"`
	EventTime    string   `json:"eventTime"`
	EventName    string   `json:"eventName"`
	S3           S3Record `json:"s3"`
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
	Key  string
	Data string
}

func (t Transformer) Download(record Record, ch chan<- Transformable) {
	fmt.Println("Downloading record...")
	downloader := s3manager.NewDownloader(&t.Session)
	buff := &aws.WriteAtBuffer{}
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: &record.S3.Bucket.Name,
		Key:    &record.S3.Object.Key,
	})

	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Downloaded record!")
	ch <- Transformable{
		Record: record,
		Data:   string(buff.Bytes()),
	}
}

func (t Transformer) Receive(ch chan<- Record) {
	var response Response
	svc := sqs.New(&t.Session)
	fmt.Println("Receiving record...")
	messages, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(t.QueueUrl),
		MaxNumberOfMessages: &t.BatchSize,
		VisibilityTimeout:   &t.Visibility,
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Received record!")
	for _, message := range messages.Messages {
		json.Unmarshal([]byte(*message.Body), &response)
	}
	if len(response.Records) > 0 {
		for _, record := range response.Records {
			fmt.Println("Adding record to queue...")
			ch <- record
			fmt.Println("Added record to queue")
		}
	} else {
		fmt.Println("No messages received.")
	}
}

func (t Transformer) Upload(upload Upload) {
	fmt.Println("Uploading record...")
	uploader := s3manager.NewUploader(&t.Session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(t.S3Egress),
		Key:    aws.String(upload.Key),
		Body:   strings.NewReader(upload.Data),
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println("Uploaded record!")
}
