import boto3
import time
import sys

from .base_step import Step


@Step( 
    type="trigger_file", 
    props_schema={ 
        "type": "object", 
        "properties":{
            "file_name" : {"type": "string"},
            "to_bucket" : {"type": "string"}
        },
        "required":["file_name","to_bucket"] 
    } 
)


class TriggerFile:
    def run_step(self, spark, config, context=None,  glueContext=None):
        
        # read props

        file_name = self.props.get("file_name")
        to_bucket = self.props.get("to_bucket")
        
        s3 = boto3.resource('s3')

        print("Bucket-", to_bucket)
        print("file_name-", file_name)

        content = "File craeted to trigger dependent job"
        s3.Object(to_bucket, file_name).put(Body=content)

        print("File created")