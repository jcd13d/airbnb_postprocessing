* need to create execution role? 
    * Trusted entity - Lambda
    * Permissions - AWSLambdaBasicExecutionRole
    * Role name - lambda-role
    * need emr also
 {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "elasticmapreduce:*",
            "Resource": "*"
        }
    ]
}
* boostrap is boilerplate creates run time 
* my function goes in function file, can make that whatever I want


# CMDS
chmod 755 function.sh bootstrap
zip function.zip function.sh bootstrap

aws lambda create-function --function-name bash-runtime \
--zip-file fileb://function.zip --handler function.handler --runtime provided \
--role arn:aws:iam::033046933810:role/lambda-emr-role

aws lambda invoke --function-name bash-runtime --payload '{"text":"Hello"}'
response.txt --cli-binary-format raw-in-base64-out

* UPDATE FUNCTION
aws lambda update-function-code --function-name bash-runtime --zip-file
fileb://function.zip

PYTHON VERSION

aws lambda create-function --function-name py-runtime \
--zip-file fileb://function.zip --handler function.handler --runtime python3.8 \
--role arn:aws:iam::033046933810:role/lambda-emr-role

zip function.zip function.py

aws lambda create-function --function-name py-runtime \
--zip-file fileb://function.zip --handler function.handler --runtime python3.8 \                              
--role arn:aws:iam::033046933810:role/lambda-emr-role

aws lambda invoke --function-name py-runtime \
--payload '{"test": "hello"}' response.txt --cli-binary-format raw-in-base64-out



