package mronaws

import (
	"bytes"
	"context"
	"io/ioutil"

	"fmt"
	"log"
	
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var AWS_UPLOADER *s3manager.Uploader

const AWS_S3_REGION string = "us-west-1"
const AWS_S3_BUCKET_NAME string = "mapreducedata"
const AWS_S3_SECRETS string = "s3_creds.csv"

func createUploader() *s3manager.Uploader {
	file, err := ioutil.ReadFile(AWS_S3_SECRETS)
	

	s3Config := &aws.Config{
		Region: 		aws.String(AWS_S3_BUCKET_NAME),
		Credentials: 	credentials.NewStaticCredentials("KeyID", "SecretKey", "")
	}

	s3Session := session.New(s3Config)
	return s3manager.NewUploader(s3Session) 
}

func AddFileToS3(filename string) {
	if AWS_UPLOADER == nil {
		AWS_UPLOADER = createUploader()
	}

	fmt.Println("Uploading...")

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	addFileInput := &s3manager.UploadInput{
		Bucket: 		aws.String(AWS_S3_BUCKET_NAME),
		Key: 			aws.String(fmt.Sprintf("output/%s", filename)),
		Body: 			bytes.NewReader(filename),
		ContentType 	aws.String("text"),
	}

	res, err := AWS_UPLOADER.UploadWithContext(context.Background(), addFileInput)
}