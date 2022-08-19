package mronaws

import "os"
import "bytes"
import "context"
import "io"
import "io/ioutil"
import "encoding/csv"

import "fmt"
import "log"
	
import "github.com/aws/aws-sdk-go/aws"
import "github.com/aws/aws-sdk-go/aws/credentials"
import "github.com/aws/aws-sdk-go/aws/session"
import "github.com/aws/aws-sdk-go/service/s3/s3manager"

var AWS_UPLOADER *s3manager.Uploader

const AWS_S3_REGION string = "us-west-1"
const AWS_S3_BUCKET_NAME string = "mapreducedata"

const AWS_S3_CREDENTIALS string = "s3_creds.csv"
const AWS_S3_ACCESS_KEY_ID_INDEX int = 2
const AWS_S3_SECRET_KEY_INDEX int = 3

func CreateUploader() *s3manager.Uploader {
	file, _ := os.Open(AWS_S3_CREDENTIALS)
	r := csv.NewReader(file)
	var prev_record []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		prev_record = record
	}

	fmt.Println("WHAT IS RECORD", prev_record)
	s3Config := &aws.Config{
		Region: 		aws.String(AWS_S3_BUCKET_NAME),
		Credentials: 	credentials.NewStaticCredentials(prev_record[AWS_S3_ACCESS_KEY_ID_INDEX], prev_record[AWS_S3_SECRET_KEY_INDEX], ""),
	}

	s3Session := session.New(s3Config)
	return s3manager.NewUploader(s3Session)
}

func AddFileToS3(filename string) (*s3manager.UploadOutput, bool) {
	if AWS_UPLOADER == nil {
		AWS_UPLOADER = CreateUploader()
	}

	fmt.Println("Uploading...")

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	addFileInput := &s3manager.UploadInput{
		Bucket: 		aws.String(AWS_S3_BUCKET_NAME),
		Key: 			aws.String(fmt.Sprintf("output/%s", filename)),
		Body: 			bytes.NewReader(file),
		ContentType: 	aws.String("text"),
	}

	uploadOutput, err := AWS_UPLOADER.UploadWithContext(context.Background(), addFileInput)
	if err != nil {
		return nil, false
	}
	return uploadOutput, true
}






