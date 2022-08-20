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
import "github.com/aws/aws-sdk-go/service/s3"
import "github.com/aws/aws-sdk-go/service/s3/s3manager"

var AWS_SESSION *session.Session
var AWS_S3 *s3.S3
var AWS_UPLOADER *s3manager.Uploader
var AWS_DOWNLOADER *s3manager.Downloader

const AWS_S3_REGION string = "us-west-1"
const AWS_S3_BUCKET_NAME string = "mapreducedata"

const AWS_S3_CREDENTIALS string = "s3_creds.csv"
const AWS_S3_ACCESS_KEY_ID_INDEX int = 2
const AWS_S3_SECRET_KEY_INDEX int = 3

func createSession() *session.Session {
	if AWS_SESSION != nil {
		return AWS_SESSION
	}

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

	s3Config := &aws.Config{
		Region: 		aws.String(AWS_S3_REGION),
		Credentials: 	credentials.NewStaticCredentials(prev_record[AWS_S3_ACCESS_KEY_ID_INDEX], prev_record[AWS_S3_SECRET_KEY_INDEX], ""),
	}
	return session.New(s3Config)
}

func createS3() *s3.S3 {
	if AWS_S3 != nil {
		return AWS_S3
	}

	return s3.New(createSession())
}

func createUploader() *s3manager.Uploader {
	if AWS_SESSION == nil {
		AWS_SESSION = createSession()
	}

	return s3manager.NewUploader(AWS_SESSION)
}

func createDownloader() *s3manager.Downloader {
	if AWS_SESSION == nil {
		AWS_SESSION = createSession()
	}

	return s3manager.NewDownloader(AWS_SESSION)
}

func AddFileToS3(filename string, prefix string) (*s3manager.UploadOutput, error) {
	if AWS_UPLOADER == nil {
		AWS_UPLOADER = createUploader()
	}

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	filepath := filename
	if prefix != "" {
		filepath = fmt.Sprintf("%s/%s", prefix, filename)
	}
	addFileInput := &s3manager.UploadInput{
		Bucket: 		aws.String(AWS_S3_BUCKET_NAME),
		Key: 			aws.String(filepath),
		Body: 			bytes.NewReader(file),
		ContentType: 	aws.String("text"),
	}

	uploadOutput, err := AWS_UPLOADER.UploadWithContext(context.Background(), addFileInput)
	return uploadOutput, err
}

func DownloadFileInS3(key string) error {
	if AWS_DOWNLOADER == nil {
		AWS_DOWNLOADER = createDownloader()
	}

	file, err := os.Create(key)
	if err != nil {
		return err
	}

	defer file.Close()

	_, err = AWS_DOWNLOADER.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(AWS_S3_BUCKET_NAME),
			Key: aws.String(key),
		},
	)
	return err
}

func ListFilesInS3(prefix string) (*s3.ListObjectsV2Output, error) {
	if AWS_S3 == nil {
		AWS_S3 = createS3()
	}

	resp, err := AWS_S3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(AWS_S3_BUCKET_NAME),
		Prefix: aws.String(prefix),
	})
	return resp, err
}

func DeleteFileInS3(key string) error {
	if AWS_S3 == nil {
		AWS_S3 = createS3()
	}

	_, err := AWS_S3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(AWS_S3_BUCKET_NAME),
		Key: aws.String(key),
	})
	if err != nil {
		return err
	}

	err = AWS_S3.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(AWS_S3_BUCKET_NAME),
		Key: aws.String(key),
	})
	if err != nil {
		return err
	}
	return nil
}






