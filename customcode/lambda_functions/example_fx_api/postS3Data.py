import json
import boto3


def handler(event, context):
    print(event)
    print('Received createLambda request')

    # Connect to S3
    s3 = boto3.client("s3")

    # Define the S3 bucket and file name
    bucket_name = "dataall-nagarro-dhcentrallogs-nczcb7va"
    file_name = "api.json"

    rollno = event['rollno']
    firstname = event['firstname']
    lastname = event['lastname']

    data = {
        "rollno": rollno,
        "firstname": firstname,
        "lastname": lastname
    }

    # Convert the data to a JSON string
    file_content = json.dumps(data)

    # Write the data to the file in S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)

    # Return a success message
    return {
        "statusCode": 200,
        "body": "Data written to S3 successfully"
    }