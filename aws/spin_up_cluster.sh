#aws emr create-cluster --applications Name=Spark Name=Zeppelin \
#--ebs-root-volume-size 10 \
#--ec2-attributes '{"KeyName":"btd-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-03ab02d97bb21bdb1","EmrManagedSlaveSecurityGroup":"sg-074c6c985a867ce44","EmrManagedMasterSecurityGroup":"sg-0cbfcd54dcb8705db"}' \
#--service-role EMR_DefaultRole \
#--enable-debugging \
#--release-label emr-5.35.0 \
#--log-uri 's3n://aws-logs-033046933810-us-east-1/elasticmapreduce/' \
#--auto-termination-policy '{"IdleTimeout":3600}' \
#--name 'cli-emr-cluster' \
#--instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' \
#--region us-east-1 \
#--bootstrap-actions Path="s3://jd-s3-test-bucket9/emr_test/set_up_cluster.sh" \
#--steps file://aws/step_config.json \
#--auto-terminate
#--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \

aws emr create-cluster --applications Name=Spark Name=Zeppelin \
--ebs-root-volume-size 10 \
--ec2-attributes '{"KeyName":"btd_key_pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0560d0f903be012ef","EmrManagedSlaveSecurityGroup":"sg-03534c3cfcd58a122","EmrManagedMasterSecurityGroup":"sg-02b22632fe2991242"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.35.0 \
--log-uri 's3://airbnb-scraper-bucket-0.0.1/emr_logs/' \
--auto-termination-policy '{"IdleTimeout":3600}' \
--name 'cli-emr-cluster' \
--instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' \
--region us-east-1 \
--bootstrap-actions Path="s3://airbnb-scraper-bucket-0-0-1/set_up_cluster.sh" \
--steps file://aws/step_config.json \
#--auto-terminate
#--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
