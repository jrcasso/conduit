package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

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

type Upload struct {
	Bucket string
	Key    string
	Data   string
}

func main() {
	var visibility, batchSize int64
	var s3Ingress, s3Egress, queueUrl string
	var response Response

	s3Ingress = os.Getenv("TRANSFORM_S3_INGRESS_BUCKET")
	s3Egress = os.Getenv("TRANSFORM_S3_EGRESS_BUCKET")
	queueUrl = os.Getenv("TRANSFORM_QUEUE_URL")
	visibility, _ = strconv.ParseInt(os.Getenv("TRANSFORM_VISIBILITY_TIMEOUT"), 10, 64)
	batchSize, _ = strconv.ParseInt(os.Getenv("TRANSFORM_BATCH_SIZE"), 10, 64)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String("us-east-1"),
			Endpoint:         aws.String("http://localstack:4566"),
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	f, _ := os.Open("go.mod")
	uploader := s3manager.NewUploader(sess)
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Ingress),
		Key:    aws.String("foo"),
		Body:   f,
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Println(result)

	svc := sqs.New(sess)
	messages, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: &batchSize,
		VisibilityTimeout:   &visibility,
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	fmt.Printf("%+v", messages)
	for _, message := range messages.Messages {
		fmt.Println(message.Body)
		json.Unmarshal([]byte(*message.Body), &response)
		fmt.Println(response)
	}
	downloader := s3manager.NewDownloader(sess)
	downloadChannel := make(chan string)
	uploadChannel := make(chan string)
	var wg sync.WaitGroup
	for _, record := range response.Records {
		wg.Add(1)
		go func(record Record, wg *sync.WaitGroup) {
			defer wg.Done()

			fmt.Println("Downloading...")
			buff := &aws.WriteAtBuffer{}
			_, err := downloader.Download(buff, &s3.GetObjectInput{
				Bucket: &record.S3.Bucket.Name,
				Key:    &record.S3.Object.Key,
			})
			if err != nil {
				panic(fmt.Sprintf("%+v", err))
			}

			data := string(buff.Bytes())
			fmt.Printf("%+v", data)
			downloadChannel <- data
			fmt.Println("Downloaded")
		}(record, &wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			fmt.Println("Transforming...")
			transformedData := transform(<-downloadChannel)
			fmt.Println("Transformed")
			fmt.Printf("%+v", transformedData)
			uploadChannel <- transformedData
		}(&wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			fmt.Println("Uploading...")
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(s3Egress),
				Key:    aws.String("foo"),
				Body:   strings.NewReader(<-uploadChannel),
			})
			if err != nil {
				panic(fmt.Sprintf("%+v", err))
			}
			fmt.Println("Uploaded")
		}(&wg)
	}
	wg.Wait()
}

func transform(data string) string {
	return fmt.Sprintf("FOO %s", data)
}

// func upload(uploader s3manager.Uploader, uploadChannel <-chan string, wg *sync.WaitGroup) {
// 	_, err := uploader.Upload(&s3manager.UploadInput{
// 		Bucket: aws.String(s3Egress),
// 		Key:    aws.String("foo"),
// 		Body:   strings.NewReader(<-uploadChannel),
// 	})
// 	if err != nil {
// 		panic(fmt.Sprintf("%+v", err))
// 	}
// }
