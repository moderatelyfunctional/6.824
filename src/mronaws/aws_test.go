package mronaws

import "fmt"
import "testing"

const TEST_OBJECT_PREFIX string = "test"
const TEST_OBJECT_NAME string = "README_TEST.md"
var TEST_OBJECT_KEY string = fmt.Sprintf("%s/%s", TEST_OBJECT_PREFIX, TEST_OBJECT_NAME)

func TestAddFileToS3(t *testing.T) {
	t.Run("AddFileToS3", func(t *testing.T) {
		_, err := AddFileToS3(TEST_OBJECT_NAME, TEST_OBJECT_PREFIX)
		if err != nil {
			t.Errorf("There was an error uploading the file to S3 %v", err)
		}
		DeleteFileInS3(TEST_OBJECT_KEY)
	})
}

func TestListFilesInS3(t *testing.T) {
	t.Run("ListFilesInS3", func(t *testing.T) {
		AddFileToS3(TEST_OBJECT_NAME, TEST_OBJECT_PREFIX)
		objects, err := ListFilesInS3(TEST_OBJECT_PREFIX)
		if err != nil {
			t.Errorf("There was an error listing the files in S3 %v", err)
		}
		if len(objects.Contents) != 1 {
			t.Errorf("Expected one object to exist. %d found", len(objects.Contents))
		}
		if *objects.Contents[0].Key != TEST_OBJECT_KEY {
			t.Errorf("Expected object to be %s but was %s", TEST_OBJECT_KEY, *objects.Contents[0].Key)
		}
		DeleteFileInS3(TEST_OBJECT_KEY)
	})
}

func TestAddListAndDeleteFileInS3(t *testing.T) {
	t.Run("DeleteFileInS3", func(t *testing.T) {
		AddFileToS3(TEST_OBJECT_NAME, TEST_OBJECT_PREFIX)
		
		objects, err := ListFilesInS3(TEST_OBJECT_PREFIX)
		if len(objects.Contents) != 1 {
			t.Errorf("Expected one object to exist. %d found", len(objects.Contents))
		}
		if *objects.Contents[0].Key != TEST_OBJECT_KEY {
			t.Errorf("Expected object to be %s but was %s", TEST_OBJECT_KEY, *objects.Contents[0].Key)
		}
		
		err = DeleteFileInS3(TEST_OBJECT_KEY)
		if err != nil {
			t.Errorf("Error %v during deletion of %s", err, TEST_OBJECT_NAME)
		}
		objects, _ = ListFilesInS3(TEST_OBJECT_PREFIX)
		if len(objects.Contents) != 0 {
			t.Errorf("Expected no objects to exist. %d found", len(objects.Contents))
		}
	})
}

func TestDownloadFileInS3(t *testing.T) {
	t.Run("DownloadFileInS3", func(t *testing.T) {
		removeFiles([]string{TEST_OBJECT_KEY})
		AddFileToS3(TEST_OBJECT_NAME, TEST_OBJECT_PREFIX)

		err := DownloadFileInS3(TEST_OBJECT_KEY)
		if err != nil {
			t.Errorf("Expected no error during download %v", err)
		}
		if !exists(TEST_OBJECT_KEY) {
			t.Errorf("Expected file %s to exist on local filesystem", TEST_OBJECT_KEY)
		}

		removeFiles([]string{TEST_OBJECT_KEY})
		DeleteFileInS3(TEST_OBJECT_KEY)
	})
}