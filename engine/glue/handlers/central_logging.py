from .base_step import Step
from .observability import StepMetric
import boto3
import json
from datetime import datetime
from .base_central_logging import BaseCentralLogging
import uuid

@Step(
    type="logging",
    props_schema={
        "type" : "object",
        "properties" : {
            "options" : {
                "type" : "object",
                "properties" : {
                    "log_type" : {"type" : "string"},
                    "status" : {"type" : "string"},
                    "central_bucket" : {"type" : "string"}
                },
                "required" : ["log_type", "status","central_bucket"],
            }
        },
    }
)
#  class creating object to log from the start and end step and calling BaseCentralLogging class
class CentralLogging :
    def run_step(self, spark, config=None, context=None, glueContext=None) :
        options = self.props.get("options", {})
        timestamp = str(datetime.now())
        print("config.args-", config.args)
        guid = (config.args.get('JOB_FLOW_ID')).split(":")[-1]
        job = config.args.get("JOB_NAME")
        pipeline = config.args.get("step_function_name")
        log_info = {
            "timestamp": timestamp,
            "guid": guid,
            "process" : pipeline,
            "job": job,
            "step" : options["log_type"],
            "eventType": "Pipeline-Job-Step",
            "status" : options["status"],
            "msg": ""
        }
        central_logging_bucket=self.props.get("options",{}).get("central_bucket","")        
        base_central_logger = BaseCentralLogging()
        base_central_logger.create_log_in_s3(config, log_info,central_logging_bucket)
        # self.create_log_in_s3(config, log_info)