import boto3
import json

client = boto3.client('emr')

def handler(event, context):
    s3 = boto3.resource('s3')
    content_object = s3.Object('airbnb-scraper-bucket-0-0-1', 'running_configs/postprocess_launch_config.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    config = json.loads(file_content)

    response = client.run_job_flow(**config)

    return response
