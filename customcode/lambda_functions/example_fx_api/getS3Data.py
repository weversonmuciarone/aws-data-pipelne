import json
import boto3


def handler(event, context):
    # Connect to S3
    s3 = boto3.client("s3")

    bucket_name = "dataall-nagarro-dhcentrallogs-nczcb7va"
    file_name = "central_logs/logs/2022-05-05 05:42:13.508094_dhcentrallogtest.json"

    # Get the data from the file in S3
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response["Body"].read().decode("utf-8")

    # Load the data as a JSON object
    data = json.loads(file_content)

    # Return the data
    return {
        "statusCode": 200,
        "body": data
    }
