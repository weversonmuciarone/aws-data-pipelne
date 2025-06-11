import json
import uuid



def handler(event, context):
    rollno = event['rollno']
    firstname = event['firstname']
    lastname = event['lastname']
    # print('with param firstname=' + firstname)
    # personId = str(uuid.uuid1())
    return {
        "rollno": rollno,
        "firstname": firstname,
        "lastname": lastname
    }