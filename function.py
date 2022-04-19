import boto3

client = boto3.client('emr')

print("Starting..\n")


def handler(event, context):
    response = client.run_job_flow(
        Name='cli-emr-cluster-lambda',
        LogUri='s3://airbnb-scraper-bucket-0-0-1/emr_logs/',
        ReleaseLabel='emr-5.35.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 2,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-02e3bd361019638af'
            # 'Ec2SubnetId': 'subnet-0e603a8b787e76dc8'
            # 'Ec2SubnetId': 'subnet-0ebd9fe1b02a38e96'
            # 'Ec2SubnetId': 'subnet-06b6292a876432928'
            # 'Ec2SubnetId': 'subnet-0af6b8799829bcd29'
            # 'Ec2SubnetId': 'subnet-0560d0f903be012ef'
        },
        Applications=[{'Name': 'Spark'}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Steps=[
            {
                'Name': 'postprocessing',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit",
                             "--py-files",
                             "s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/dependencies.zip",
                             "s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/postprocessing.py"]
                }
            }
        ],
        BootstrapActions=[
            {
                "Name": "EMR Setup",
                "ScriptBootstrapAction": {
                    "Path": "s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/set_up_cluster.sh"
                }
            }
        ]
    )

    return response
