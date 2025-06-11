import boto3
from datetime import date
from .base_step import Step
from .base_central_logging import BaseCentralLogging
from datetime import datetime

@Step(
    type="validate_file",
    props_schema={
        "type":  "object",
        "properties":{
            "bucket" : {"type": "string"},
            "prefix" : {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required":["bucket","prefix"]
    }
)
class FileValidator:

    def __init__(self, **config):

        self.s3 = boto3.client("s3")
        super().__init__(**config)

    def run_step(self, spark,config=None,context=None, glueContext=None):

        job_name = config.args.get('JOB_NAME')
        bucket = self.props.get("bucket")
        prefix_list = self.props.get("prefix")
        prefixkey_not_found = []

        for prefix in prefix_list:

            kwargs = {'Bucket': bucket,
                      "MaxKeys": 1}
            if isinstance(prefix, str):
                kwargs['Prefix'] = prefix
            get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
            objs_response = self.find_key(prefix, **kwargs)

            try:
                contents = objs_response['Contents']
            except KeyError:
                return "Wrong key name in prefix."
            all_key_names = [obj['Key'] for obj in sorted(contents, key=get_last_modified, reverse=True)]
            print("List of keys present in Prefix {prefix} is :\n{all_key_names}".format(prefix = prefix, all_key_names = all_key_names))

            curr_day_key = all_key_names[0]
            print("Name of current file in Prefix {prefix} is {curr_day_key}".format(prefix = prefix, curr_day_key = curr_day_key))

            prefix_split = prefix.split('/')
            if curr_day_key == prefix + '/':
                prefixkey_not_found.append(prefix)
                continue
            else:
                prefix_split = prefix_split[1]

            current_date = str(date.today()).replace('-','')
            date_from_key = curr_day_key[len(prefix) + len(prefix_split) + 1: len(prefix) + len(prefix_split) + 9]
            print("Date Key from filename is : ", date_from_key)

            if date_from_key == current_date:
                print("Current date is equal to file key date")
            else:
                prefixkey_not_found.append(prefix)
                print("Data not available for current date in S3")

        if len(prefixkey_not_found) > 0 :
            print("Files for extractors {prefix_not_found} not found.".format(prefix_not_found = prefixkey_not_found))
            raw_step_status = str(self.status)
            step_status = raw_step_status.split(".")[1]
            timestamp = str(datetime.now())
            guid = (config.args.get('JOB_FLOW_ID')).split(":")[-1]
            # Object to log out for step
            log_info = {
                "timestamp": timestamp,
                "guid": guid,
                "process": config.args.get("step_function_name"),
                "job": config.args.get("JOB_NAME"),
                "step": self.name,
                "eventType": "Pipeline-Job-Validate",
                "status": step_status,
                "msg": "Files for extractors {prefix_not_found} not found.".format(prefix_not_found = prefixkey_not_found)
            }
            base_central_logger = BaseCentralLogging()
            base_central_logger.create_log_in_s3(config, log_info)

        else:
            print("**** All files are present for {todays_date}".format(todays_date = date.today()))
        return

    def find_key(self, prefix ,**kwargs):

        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix + '/'
        print("******", kwargs)
        objs_response = self.s3.list_objects_v2(**kwargs)

        while objs_response['IsTruncated'] == True:
            kwargs['ContinuationToken'] = objs_response['NextContinuationToken']
            objs_response = self.s3.list_objects_v2(**kwargs)

        return objs_response