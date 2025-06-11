import json
import uuid



def handler(event, context):
    data = {
        "rollno": 1,
        "firstname": "vipul",
        "lastname": "rastogi"
    }
    return {
            "statusCode": 200,
            "body": data
    }