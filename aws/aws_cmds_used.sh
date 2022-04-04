aws emr create-cluster \
  --name test-emr-cluster \
  --use-default-roles \
  --release-label emr-5.28.0 \
  --instance-count 3 \
  --instance-type m5.xlarge \
  --applications Name=JupyterHub Name=Spark Name=Hadoop \
  --ec2-attributes KeyName=emr-cluster  \
  --log-uri s3://s3-for-emr-cluster/

