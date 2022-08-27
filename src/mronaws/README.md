## High Level Architecture of MapReduce on AWS:
#### 1) Create an S3 bucket named mapreducedata with the following folders:
- input
	- `pg-being_ernest.txt`
	- `pg-dorian_gray.txt`
	...
	- `pg-tom_sawyer.txt`
- intermediate
	- `mr-0-0`
	- `mr-0-1`
	...
	- `mr-0-r` (r = number of reduce tasks)
	...
	- `mr-m-r` (m = number of map tasks)
- output
	- `mr-out-0`
	- `mr-out-1`
	...
	- `mr-out-r` (r = number of reduce tasks)
Be sure to populate the input directory.

#### 2) Create an IAM role with S3 read/write privileges. Keep track of the Access Key ID and Secret Access Key
- Integrate the S3 credentials into the AWS Golang SDK (for the application).
	- `mronaws/aws.go`
		- `AddFileToS3`
			- [APPLICATION] Map/Reduce workers upload their output files to S3
		- `DownloadFileInS3`
			- [APPLICATION] Map/Reduce workers fetch the input files they work on.
		- `ListFilesInS3`
			- [APPLICATION] Coordinator checks the status of Map/Reduce tasks based on file existence 
		- `DeleteFileInS3`
			- [TESTING] reset S3 state between runs.
- Integrate the aws CLI tools for testing (for the bash scripts). After every test run:
	- `aws s3 rm s3://mapreducedata/intermediate --recursive --exclude "."`
	- `aws s3 rm s3://mapreducedata/output --recursive --exclude "."`

#### 3) Create EC2 Instances For the Coordinator/Worker(s)
Choose an availability zone that's identical to the S3 zone to reduce network latency.
- Spin up an EC2 instance for the coordinator. Be sure to open inbound port `1234` for communication.
- Spin up an EC2 instance for each worker. Be sure to open outbound port `1234` for communication.

Port configurations are managed via security groups.

The coordinator must be started available first, otherwise the workers will quit.

#### 4) Implementation Differences Against Local MapReduce
Some differences against the original MapReduce implementation in `package mr` where:
- The coordinator and the workers reside on the same physical machine so
	- RPCs are executed over a local socket name
	- The data (input, intermediate, and output files) can be accessed via os file operations,
	which also provide atomicity benefits.

In MapReduce on AWS:
- The coordinator and each worker reside on different physical machines so
	- RPCs are executed over the network
	- The data (input, intermediate and output files) are stored on S3.
	
#### 5) To test for correctness, check the test scripts
The test scripts are similar to the original MapReduce implementation with some differences:
- `test-mr-wc-on-aws.sh`
	- [BASE]
- `test-mr-indexer-on-aws.sh`
	- [BASE]
- `test-mr-jobcount-on-aws.sh`
	- [BASE]
- `test-mr-mtiming-on-aws.sh`
	- [BASE]
- `test-mr-rtiming-on-aws.sh`
	- [BASE]
- `test-mr-early-exit-on-aws.sh`
	- [BASE]
- `test-mr-crash-on-aws.sh`
	- [BASE] Checks for whether server port is bound (:1234) as opposed to the socket. Sort also checks for 
	`*/mr-out*` rather than `mr-out*` because crashed reduce workers won't copy their output to the base directory.

