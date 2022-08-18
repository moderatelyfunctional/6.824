To run MapReduce on AWS:
1) Create an S3 bucket and create an IAM user with access to the bucket.
2) Spin up N EC2 instances, 1 of which runs the Coordinator, the rest of which run the Worker.
	- The Coordinator must be started first, otherwise the Workers will exit.
3) The Worker processes will exit once the Coordinator changes to the COORDINATOR_DONE state
4) Output data should be stored on S3.