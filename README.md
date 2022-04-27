# airbnb_postprocessing

## Usage
The postprocessing package is meant to be run on AWS EMR. The current 
structure is an eventBridge event that triggers a lambda function that 
runs the EMR job. This currently uses postprocessing.py as "main" for 
the postprocessing jobs, but you could also write other jobs and add 
them to the EMR configuration as a step to be run. (if it follows the
same frequency as the postprocessing)

### IAM Roles
Everyone's favorite part in working with AWS! I just give full EMR access to
the lambda function. I created a new policy wil full access to EMR and added
it to a role that I then included on the lambda function along with these
policies:
* AmazonElasticMapReduceFullAccess
* AWSLambdaExecute
* AWSLambdaBasicExecutionRole

### EMR
Elastic Map Reduce is an EC2 instance with all the required software to run spark and 
other big-data frameworks. We want to run an ephemeral instance that spins up, does its
job, and terminates. The configuration passed to the lambda function described below
does this. 

The folowing script can be used to zip and push the necessary postprocessing files to 
the appropriate locations in s3.  
```commandline
>./bin/build.sh
```

TODO ADD EXPLAINER ON CONFIG

### Lambda
The lambda function simply starts an EMR session and passes the necessary configuration.
The lambda function is created with a .zip file that contains the python script we will
run to start EMR. To create the file, run the following command from the root directory
of this repo:
```commandline
zip ./function.zip ./function.py
```
The script can then be deployed in a lambda function as follows:
```commandline
aws lambda create-function --function-name launch-postprocessor \
--zip-file fileb://function.zip --handler function.handler --runtime python3.8 \
--role arn:aws:iam::443188464014:role/lambda-emr-role --timeout 20
```
To update the lambda function code after changes, run this command:
```commandline
aws lambda update-function-code --function-name launch-postprocessor --zip-file fileb://function.zip
```
A note: at some point I needed to create the default EMR roles, if you run into
issues try running this. 
```commandline
aws emr create-default-roles
```

### EventBridge
The EventBridge event consists of the "rule" and the "target". The Rule is what you schedule
and the target is what happens when the rule is triggered. A rule can be created as follows:
```commandline
aws events put-rule --name "weekly-postprocessor" --schedule-expression "cron(0 4 * * ? *)"
```
[Info on the cron expression here.](https://en.wikipedia.org/wiki/Cron) The expression 
defines when the job runs and in what frequency.

The target can be added using the following expression:
```commandline
aws events put-targets --rule "weekly-postprocessor" --cli-input-json file://aws/eventbridge_target.json
```
The "eventbridge_target.json" contains information to configure the target. In this case 
it has the rule being applied to and the ARN of the lambda function to trigger.
[More info on syntax here.](https://docs.aws.amazon.com/cli/latest/reference/events/put-targets.html)

