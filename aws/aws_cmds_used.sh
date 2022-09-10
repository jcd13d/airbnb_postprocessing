aws emr create-cluster \
  --name test-emr-cluster \
  --use-default-roles \
  --release-label emr-5.28.0 \
  --instance-count 3 \
  --instance-type m5.xlarge \
  --applications Name=JupyterHub Name=Spark Name=Hadoop \
  --ec2-attributes KeyName=emr-cluster  \
  --log-uri s3://s3-for-emr-cluster/


# set up cluster
aws emr create-cluster --bostrap-actions Path="s3://jd-s3-test-bucket9/emr_test/set_up_cluster.sh" --steps file://aws/step_config.json

aws emr create-cluster --applications Name=Spark Name=Zeppelin --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"first-ec2-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-03ab02d97bb21bdb1","EmrManagedSlaveSecurityGroup":"sg-074c6c985a867ce44","EmrManagedMasterSecurityGroup":"sg-0cbfcd54dcb8705db"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.35.0 --log-uri 's3n://aws-logs-033046933810-us-east-1/elasticmapreduce/' --auto-termination-policy '{"IdleTimeout":3600}' --name 'cli-emr-cluster' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1 --bostrap-actions Path="s3://jd-s3-test-bucket9/emr_test/set_up_cluster.sh" --steps file://aws/step_config.json
aws emr create-cluster --applications Name=Spark Name=Zeppelin \
--ebs-root-volume-size 10 \
--ec2-attributes '{"KeyName":"first-ec2-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-03ab02d97bb21bdb1","EmrManagedSlaveSecurityGroup":"sg-074c6c985a867ce44","EmrManagedMasterSecurityGroup":"sg-0cbfcd54dcb8705db"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.35.0 \
--log-uri 's3n://aws-logs-033046933810-us-east-1/elasticmapreduce/' \
--auto-termination-policy '{"IdleTimeout":3600}' \
--name 'cli-emr-cluster' \
--instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-east-1 \
--bootsrap-actions Path="s3://jd-s3-test-bucket9/emr_test/set_up_cluster.sh" \
--steps file://aws/step_config.json


spark-submit --py-files s3://jd-s3-test-bucket9/emr_test/dependencies.zip s3://jd-s3-test-bucket9/emr_test/postprocessing.py


# copy files to s3 location
aws s3 cp config s3://jd-s3-test-bucket9/test_configs/ --recursive
aws s3 cp ./aws/set_up_cluster.sh s3://jd-s3-test-bucket9/emr_test/set_up_cluster.sh
aws s3 cp ./spark_postprocessor.py s3://jd-s3-test-bucket9/emr_test/spark_postprocessor.py
aws s3 cp ./build/dependencies/dependencies.zip s3://jd-s3-test-bucket9/emr_test/dependencies.zip
aws s3 cp ./postprocessing.py s3://jd-s3-test-bucket9/emr_test/postprocessing.py


aws s3 cp ./spark_postprocessor.py s3://jd-s3-test-bucket9/emr_test/spark_postprocessor.py

zip -r build/dependencies/dependencies.zip postprocessor

# to get ip address
aws emr list-clusters
aws emr describe-cluster --cluster-id j-1LVRT4I8731SL

# add ip to security for master node

# scp keys to master node so I can use git

# ec2-54-87-242-119.compute-1.amazonaws.com
aws emr ssh --cluster-id j-30EBOPNE7THT0 --key-pair-file ~/first-ec2-key-pair.pem
ssh -i ~/first-ec2-key-pair.pem hadoop@ec2-35-175-249-148.compute-1.amazonaws.com


# send keys for github
scp -i ~/.ssh/first-ec2-key-pair.pem ~/.ssh/id_ed25519 hadoop@ec2-35-175-249-148.compute-1.amazonaws.com:~/.ssh/id_ed25519

# run on cluster
sudo yum update
sudo yum install git
sudo yum install tmux

git clone git@github.com:jcd13d/airbnb_postprocessing.git

# tunnel 8088 to 8157 on localhost (this is the UI, would do another port for zeppelin)
ssh -i ~/.ssh/first-ec2-key-pair.pem -N -L 8157:ec2-35-175-249-148.compute-1.amazonaws.com:8088 hadoop@ec2-35-175-249-148.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:hadoop@ec2-54-85-42-226.compute-1.amazonaws.com:8088 hadoop@ec2-54-85-42-226.compute-1.amazonaws.com
hadoop@ec2-54-85-42-226.compute-1.amazonaws.com
# Zeppelin
ssh -i ~/.ssh/first-ec2-key-pair.pem -N -L 8170:ec2-100-24-51-109.compute-1.amazonaws.com:8890 hadoop@ec2-100-24-51-109.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:hadoop@ec2-54-85-42-226.compute-1.amazonaws.com:8088 hadoop@ec2-54-85-42-226.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:hadoop@ec2-44-200-28-195.compute-1.amazonaws.com:8890 hadoop@ec2-44-200-28-195.compute-1.amazonaws.com
ec2-44-200-28-195.compute-1.amazonaws.com:8890/
ssh -i ~/.ssh/btd_key_pair.pem -ND 8157 ec2-44-200-28-195.compute-1.amazonaws.com:8890
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:ec2-54-197-33-188.compute-1.amazonaws.com:8890 hadoop@ec2-54-197-33-188.compute-1.amazonaws.com
ec2-34-229-203-169.compute-1.amazonaws.com
ec2-54-197-33-188.compute-1.amazonaws.com
ec2-54-197-33-188.compute-1.amazonaws.com


# FINALLY WORKED
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:ec2-34-229-203-169.compute-1.amazonaws.com:8088 hadoop@ec2-34-229-203-169.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:ec2-34-229-203-169.compute-1.amazonaws.com:8890 hadoop@ec2-34-229-203-169.compute-1.amazonaws.com

ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:ec2-54-158-20-37.compute-1.amazonaws.com:8890 hadoop@ec2-54-158-20-37.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8157:ec2-54-227-91-241.compute-1.amazonaws.com:8890 hadoop@ec2-54-227-91-241.compute-1.amazonaws.com
ssh -i ~/.ssh/btd_key_pair.pem -N -L 8158:ec2-54-227-91-241.compute-1.amazonaws.com:8088 hadoop@ec2-54-227-91-241.compute-1.amazonaws.com
ec2-54-227-91-241.compute-1.amazonaws.com
ssh -i ~/btd_key_pair.pem hadoop@ec2-54-158-20-37.compute-1.amazonaws.com

ssh -i ~/.ssh/btd_key_pair.pem -N -L 8156:ec2-54-235-41-107.compute-1.amazonaws.com:8890 hadoop@ec2-54-235-41-107.compute-1.amazonaws.com

# bootstrap actions to run installation when you spin up cluster
# https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html

# for installing packages w zeppelin
# https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/

# for automating jobs
# https://aws.amazon.com/blogs/big-data/automating-emr-workloads-using-aws-step-functions/

# going to want to use custom jar spark submit probably
# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html

# run from lambda function
# https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/launch-a-spark-job-in-a-transient-emr-cluster-using-a-lambda-function.html

# s3://aws-logs-033046933810-us-east-1/elasticmapreduce/j-1JBF1GGNTOW1X/node/i-013d01409fa2450a0/bootstrap-actions/1/stderr.gz

# LAMBDA STUFF
aws emr create-default-roles
zip ./build/function.zip ./aws/function.py
zip ./function.zip ./function.py

aws lambda create-function --function-name launch-postprocessor \
--zip-file fileb://function.zip --handler function.handler --runtime python3.8 \
--role arn:aws:iam::443188464014:role/lambda-emr-role --timeout 20

./build/build.sh

aws lambda update-function-configuration --function-name launch-postprocessor --handler aws.function.handler

aws lambda update-function-code --function-name launch-postprocessor --zip-file fileb://function.zip

aws lambda invoke --function-name launch-postprocessor --payload '{"test": "hello"}' aws/response.txt --cli-binary-format raw-in-base64-out

aws s3api create-bucket --bucket airbnb-scraper-bucket-0-0-1
aws s3 cp s3://airbnb-scraper-bucket-0.0.1/postprocessing_files/set_up_cluster.sh s3://airbnb-scraper-bucket-123123123/set_up_cluster.sh
aws s3 cp s3://airbnb-scraper-bucket-0.0.1/postprocessing_files/set_up_cluster.sh s3://airbnb-scraper-bucket-testuseast1/set_up_cluster.sh
aws s3 cp s3://airbnb-scraper-bucket-0.0.1/postprocessing_files/set_up_cluster.sh s3://bucket-airbnb-test-10.0.1/set_up_cluster.sh

aws s3 cp s3://airbnb-scraper-bucket-0.0.1/ s3://airbnb-scraper-bucket-0-0-1/ --recursive

aws events put-rule --name "weekly-postprocessor" --schedule-expression "cron(0 4 * * ? *)"
aws events put-targets --rule "weekly-postprocessor" --cli-input-json file://aws/eventbridge_target.json

aws s3 cp ./temp_investigate.py s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/test_delta.py
aws s3 cp ./temp_investigate.py s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/test_delta.py
aws s3 cp ./investigate_reviews.py s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/investigate_reviews.py

spark-submit --packages io.delta:delta-core_2.12:2.0.0 s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/test_delta.py
spark-submit --packages io.delta:delta-core_2.12:2.0.0 s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/investigate_reviews.py

spark-submit --packages io.delta:delta-core_2.12:2.0.0 s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/optimize_test.py
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/postprocessing.py
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/listings_postprocessor.py
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/unique_ids.py

spark-submit --packages io.delta:delta-core_2.12:2.0.0 s3://airbnb-scraper-bucket-0-1-1/data/sandbox/pipelines_scripts/occupancy_process_justin_ex.py
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/listings_postprocessor.py > log1.txt 2>log2.txt
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/postprocessing.py > log1.txt 2>log2.txt
spark-submit --packages io.delta:delta-core_2.12:2.0.0 --py-files s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/dependencies.zip s3://airbnb-scraper-bucket-0-1-1/postprocessing_files/unique_ids.py > log1.txt 2>log2.txt
