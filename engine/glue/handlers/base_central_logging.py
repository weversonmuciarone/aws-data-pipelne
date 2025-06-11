from .observability import StepMetric
import boto3
import json
from datetime import datetime, timedelta

# Base class for creating log file in central logging bucket
class BaseCentralLogging :
    def create_log_in_s3(self, config=None, log_info=None,central_logging_bucket=None):
        # central_logging_bucket = config.args.get("central_logging_bucket")
        
        timestamp = str(datetime.now())
        log_info.__setitem__("timestamp", timestamp)
        file_name = timestamp + "_" + config.args.get("step_function_name") + ".json"
        path = ""
        prefix = "central_logs/logs"
        central_logging_dir = f"{prefix}"
        path = f"{central_logging_dir}/{file_name}"

        client = boto3.client('s3')
        print(str(json.dumps(log_info)))
        print(path)
        print(central_logging_bucket)
        client.put_object(Body=str(json.dumps(log_info)), Bucket=central_logging_bucket, Key=path)