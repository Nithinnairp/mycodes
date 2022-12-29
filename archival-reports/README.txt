---------------
Release Steps:
---------------
1. [One Time Setup] Add the user with programmatic access that will deploy it to a user group that contains roles as stated below:
	a. Amazons3FullAccess
	b. AmazonEventBridgeFullAccess
	c. AWSCloudFormationFullAccess
	d. AWSLambda_FullAccess
2. Setup local aws cli to use this new generic user by using "aws configure" command. 
	a. Set the access key and secret key of the user.
	b. Set region to ap-southeast-1
	c. Output format just put json
3. Clone archival-report code repo branch to local. Zip the content of each report following each folder name. Note that the zip file content should be the python file, NOT folder.
4. Copy the zip package of each lambda function to S3 bucket: ks-lambda-deploy-package-prod/<ENVIRONMENT>/archival-report
5. Run "aws cloudformation deploy --template-file archival_report_release.yml --stack-name archival-report-lambda-stack-<ENVIRONMENT>"

NOTE 1: Update the archival_report_release.yml accordingly if you change the S3 bucket path for the zip artifacts or any other configuration. 
NOTE 2: If you only have changes in zip archive, you will need to run "aws cloudformation delete-stack --stack-name archival-report-lambda-stack-<ENVIRONMENT>" before you can run the deploy command in Step 5.
