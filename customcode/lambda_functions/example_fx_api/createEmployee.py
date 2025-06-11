import json
import uuid

CREATE_RAW_PATH = "/createEmployee"

def handler(event, context):
    print(event)
    if event['path'] == CREATE_RAW_PATH:
        print('Received createEmployee request')
        decodedBody = json.loads(event['body'])
        firstname = decodedBody['firstname']
        print('with param firstname=' + firstname)
        personId = str(uuid.uuid1())
        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "body": f"""{"person_id": {personId}}"""
        }