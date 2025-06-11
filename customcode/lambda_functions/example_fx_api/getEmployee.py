import json
import uuid

GET_RAW_PATH = "/employees"

def handler(event, context):
    print(event)
    if event['path'] == GET_RAW_PATH:
        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "body": """[
                        { "firstName": "Daniel ", "lastName": "G", "email": "myEmail@gmail.com" },
                        { "firstName": "Daniel ", "lastName": "G", "email": "myEmail@gmail.com" }
                    ]"""
        }
    else:
        return {
            "isBase64Encoded": False,
            "statusCode": 400,
        }