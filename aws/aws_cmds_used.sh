aws emr create-cluster \
  --name test-emr-cluster \
  --use-default-roles \
  --release-label emr-5.28.0 \
  --instance-count 3 \
  --instance-type m5.xlarge \
  --applications Name=JupyterHub Name=Spark Name=Hadoop \
  --ec2-attributes KeyName=emr-cluster  \
  --log-uri s3://s3-for-emr-cluster/




# to get ip address
aws emr list-clusters
aws emr describe-cluster --cluster-id j-1LVRT4I8731SL

# add ip to security for master node

# scp keys to master node so I can use git

# ec2-54-87-242-119.compute-1.amazonaws.com
aws emr ssh --cluster-id j-1LVRT4I8731SL --key-pair-file ~/first-ec2-key-pair.pem
ssh hadoop@ec2-###-##-##-###.compute-1.amazonaws.com -i ~/mykeypair.pem

# send keys for github
scp -i ~/.ssh/first-ec2-key-pair.pem ~/.ssh/id_ed25519 hadoop@ec2-54-87-242-119.compute-1.amazonaws.com:~/.ssh/id_ed25519

# run on cluster
sudo yum update
sudo yum install git
sudo yum install tmux

git clone git@github.com:jcd13d/airbnb_postprocessing.git

git clone git@github.com:jcd13d/airbnb_postprocessing.git

ssh -i ~/.ssh/first-ec2-key-pair.pem -N -L 8157:ec2-54-87-242-119.compute-1.amazonaws.com:8088 hadoop@ec2-54-87-242-119.compute-1.amazonaws.com