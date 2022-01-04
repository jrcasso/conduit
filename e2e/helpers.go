package conduit

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type TestStruct struct {
	TestString string
	TestInt    int
	TestBool   bool
}

func expectEqualInts(value int, expectation int, t *testing.T) {
	if value != expectation {
		t.Errorf("Failed: expected %d, but found %d", value, expectation)
	}
}

func expectEqualStrings(value string, expectation string, t *testing.T) {
	if value != expectation {
		t.Errorf("Failed: expected %s, but found %s", value, expectation)
	}
}

func describe(functionName string, t *testing.T) {
	t.Logf("%s:", functionName)
}

func it(description string, t *testing.T) {
	t.Logf("    - it %s", description)
}

func context(description string, t *testing.T) {
	t.Logf(" - when %s", description)
}

func generateFiles(bucket string, count int, filePrefix string) {
	var wg sync.WaitGroup
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String("us-east-1"),
			Endpoint:         aws.String("http://localstack:4566"),
		},
	}))
	uploader := s3manager.NewUploader(sess)

	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(j int, uploader *s3manager.Uploader, filePrefix string) {
			defer wg.Done()

			data := TestStruct{
				TestBool:   j == 1,
				TestString: "foo",
				TestInt:    j,
			}
			human_enc, err := json.Marshal(data)
			if err != nil {
				fmt.Println(err)
			}

			key := fmt.Sprintf("%s%d", filePrefix, j)
			body := string(human_enc)
			uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader(body),
			})
		}(i, uploader, filePrefix)
	}
	wg.Wait()
}
