To run MapReduce on AWS:
1) Create an S3 bucket named mapreducedata with the following folders:
	- input
		- pg-being_ernest.txt
		- pg-dorian_gray.txt
		...
		- pg-tom_sawyer.txt
	- intermediate
		- mr-0-0
		- mr-0-1
		...
		- mr-0-r (r = number of reduce tasks)
		...
		- mr-m-r (m = number of map tasks)
	- output
		- mr-out-0
		- mr-out-1
		...
		- mr-out-r (r = number of reduce tasks)
Be sure to populate the input directory.
2) Create an IAM role with S3 privileges.
2) Spin up N EC2 instances, 1 of which runs the Coordinator, the rest of which run the Worker.
	- The Coordinator must be started first, otherwise the Workers will exit.
3) The Worker processes will exit once the Coordinator changes to the COORDINATOR_DONE state
4) Output data should be stored on S3.