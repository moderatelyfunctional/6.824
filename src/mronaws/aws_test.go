package mronaws

import "fmt"
import "testing"

const TEST_OBJECT_PREFIX string = "test"
const LOCAL_TEST_OBJECT_FILENAME string = "README_TEST.md"
var REMOTE_TEST_OBJECT_FILENAME string = fmt.Sprintf("%s/%s", TEST_OBJECT_PREFIX, LOCAL_TEST_OBJECT_FILENAME)

func TestAWSAddFileToS3(t *testing.T) {
	t.Run("AddFileToS3", func(t *testing.T) {
		_, err := AddFileToS3(LOCAL_TEST_OBJECT_FILENAME, REMOTE_TEST_OBJECT_FILENAME)
		if err != nil {
			t.Errorf("There was an error uploading the file to S3 %v", err)
		}
		DeleteFileInS3(REMOTE_TEST_OBJECT_FILENAME)
	})
}

func TestAWSListFilesInS3(t *testing.T) {
	t.Run("ListFilesInS3", func(t *testing.T) {
		AddFileToS3(LOCAL_TEST_OBJECT_FILENAME, REMOTE_TEST_OBJECT_FILENAME)
		objects, err := ListFilesInS3(TEST_OBJECT_PREFIX)
		if err != nil {
			t.Errorf("There was an error listing the files in S3 %v", err)
		}
		if len(objects.Contents) != 1 {
			t.Errorf("Expected one object to exist. %d found", len(objects.Contents))
		}
		if *objects.Contents[0].Key != REMOTE_TEST_OBJECT_FILENAME {
			t.Errorf("Expected object to be %s but was %s", REMOTE_TEST_OBJECT_FILENAME, *objects.Contents[0].Key)
		}
		DeleteFileInS3(REMOTE_TEST_OBJECT_FILENAME)
	})
}

func TestAWSAddListAndDeleteFileInS3(t *testing.T) {
	t.Run("DeleteFileInS3", func(t *testing.T) {
		AddFileToS3(LOCAL_TEST_OBJECT_FILENAME, REMOTE_TEST_OBJECT_FILENAME)
		
		objects, err := ListFilesInS3(TEST_OBJECT_PREFIX)
		if len(objects.Contents) != 1 {
			t.Errorf("Expected one object to exist. %d found", len(objects.Contents))
		}
		if *objects.Contents[0].Key != REMOTE_TEST_OBJECT_FILENAME {
			t.Errorf("Expected object to be %s but was %s", REMOTE_TEST_OBJECT_FILENAME, *objects.Contents[0].Key)
		}
		
		err = DeleteFileInS3(REMOTE_TEST_OBJECT_FILENAME)
		if err != nil {
			t.Errorf("Error %v during deletion of %s", err, LOCAL_TEST_OBJECT_FILENAME)
		}
		objects, _ = ListFilesInS3(TEST_OBJECT_PREFIX)
		if len(objects.Contents) != 0 {
			t.Errorf("Expected no objects to exist. %d found", len(objects.Contents))
		}
	})
}

func TestAWSDownloadFileInS3(t *testing.T) {
	t.Run("DownloadFileInS3", func(t *testing.T) {
		removeFiles([]string{REMOTE_TEST_OBJECT_FILENAME})
		AddFileToS3(LOCAL_TEST_OBJECT_FILENAME, REMOTE_TEST_OBJECT_FILENAME)

		err := DownloadFileInS3(REMOTE_TEST_OBJECT_FILENAME, REMOTE_TEST_OBJECT_FILENAME)
		if err != nil {
			t.Errorf("Expected no error during download %v", err)
		}
		if !exists(REMOTE_TEST_OBJECT_FILENAME) {
			t.Errorf("Expected file %s to exist on local filesystem", REMOTE_TEST_OBJECT_FILENAME)
		}

		removeFiles([]string{REMOTE_TEST_OBJECT_FILENAME})
		DeleteFileInS3(REMOTE_TEST_OBJECT_FILENAME)
	})
}