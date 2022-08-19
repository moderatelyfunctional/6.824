package mronaws

import "fmt"
import "testing"

func TestAWSCreateUploader(t *testing.T) {
	s3Uploader := CreateUploader()
	fmt.Println(s3Uploader)
}