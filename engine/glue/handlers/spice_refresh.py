import time
from datetime import datetime

import boto3
import re
from botocore.exceptions import ClientError

from .base_step import Step


@Step(
    type="spice_refresh",
    props_schema={
        "type" : "object",
        "properties" : {
            "accountId" : {"type" : "string"},
            "dataSetId" : {"type" : "string"},
        },
        "required" : ["accountId", "dataSetId"],
    }
)
class SpiceRefresh :

    def create_spice_ingestion(self, client, dataSetId, account_id, prefix, counter) :
        dt_string = datetime.now().strftime("%Y%m%d-%H%M%S")
        ingestionId = dt_string + '-' + dataSetId

        response_di = {}
        try :
            response_ci = client.create_ingestion(DataSetId=dataSetId, IngestionId=ingestionId, AwsAccountId=account_id)
            print("refresh Ingestion status - status {0}".format(response_ci.get("IngestionStatus")))
            throttling_error_code = "ThrottlingException"
            while (response_di.get('Ingestion', {}).get('IngestionStatus')) != 'COMPLETED' :
                response_di = client.describe_ingestion(DataSetId=dataSetId, IngestionId=ingestionId, AwsAccountId=account_id)
                if response_di['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING') :
                    print("refresh Ingestion status - status {0}".format(response_di.get("Ingestion", {}).get("IngestionStatus")))
                    time.sleep(30)  # change sleep time according to your dataset size
                elif response_di['Ingestion']['IngestionStatus'] == 'COMPLETED' :
                    print(
                        "refresh completed. RowsIngested {0}, RowsDropped {1}, IngestionTimeInSeconds {2}, "
                        "IngestionSizeInBytes {3}".format(
                            response_di['Ingestion']['RowInfo']['RowsIngested'],
                            response_di['Ingestion']['RowInfo']['RowsDropped'],
                            response_di['Ingestion']['IngestionTimeInSeconds'],
                            response_di['Ingestion']['IngestionSizeInBytes']))
                    break
                else :
                    self.logger.info(f"{prefix} Failed Spice Refresh of {account_id}.{dataSetId}")
                    print("Error Type = {0}, Error Message = {1}".format(
                        response_di['Ingestion']['ErrorInfo']['Type'],
                        response_di['Ingestion']['ErrorInfo']['Message']))
                    if (throttling_error_code in response_di['Ingestion']['ErrorInfo']['Message']) & \
                            (counter > 0):
                        time.sleep(30)
                        counter = counter-1
                        self.create_spice_ingestion(client, dataSetId, account_id, prefix, counter)
                    elif (throttling_error_code in response_di['Ingestion']['ErrorInfo']['Message']) & \
                            (counter <= 0):
                        print("refresh failed! - status {0}".format(response_di['Ingestion']['IngestionStatus']))
                        raise Exception(
                            "refresh failed! retry reached to maximum limit - status {0}".format(response_di['Ingestion']['IngestionStatus']))
                    else:
                        print("refresh failed! - status {0}".format(response_di['Ingestion']['IngestionStatus']))
                        raise Exception("refresh failed! - status {0}".format(response_di['Ingestion']['IngestionStatus']))
        except ClientError as e:
            print(e.response['Error']['Code'])
            if ((e.response['Error']['Code'] == "Throttling") | \
                    (e.response['Error']['Code'] == "ThrottlingException") | \
                    (e.response['Error']['Code'] == "ThrottledException") | \
                    (e.response['Error']['Code'] == "RequestThrottledException") | \
                    (e.response['Error']['Code'] == "TooManyRequestsException") | \
                    (e.response['Error']['Code'] == "RequestLimitExceeded") | \
                    (e.response['Error']['Code'] == "LimitExceededException")) & (self.counter > 0):
                time.sleep(30)
                counter = counter-1
                self.create_spice_ingestion(client, dataSetId, account_id, prefix, counter)
            elif ((e.response['Error']['Code'] == "Throttling") | \
                    (e.response['Error']['Code'] == "ThrottlingException") | \
                    (e.response['Error']['Code'] == "ThrottledException") | \
                    (e.response['Error']['Code'] == "RequestThrottledException") | \
                    (e.response['Error']['Code'] == "TooManyRequestsException") | \
                    (e.response['Error']['Code'] == "RequestLimitExceeded") | \
                    (e.response['Error']['Code'] == "LimitExceededException")) & (self.counter <= 0):
                print("refresh failed! - status {0}".format(response_di['Ingestion']['IngestionStatus']))
                raise Exception(
                    "refresh failed! retry reached to maximum limit - status {0}".format(
                        response_di['Ingestion']['IngestionStatus']))
            else:
                print("boto3 error!")
                raise Exception("refresh failed!")

    def run_step(self, spark, config=None, context=None, glueContext=None) :
        prefix = f"{self.name} [{self.type}]"
        job_name = config.args.get("JOB_NAME")

        account_id = self.props.get("accountId")
        dataSetId = self.props.get("dataSetId")
        counter = 8
        client = boto3.client('quicksight')
        self.logger.info(f"{prefix} Attempting Spice Refresh of {account_id}.{dataSetId}")
        self.create_spice_ingestion(client, dataSetId, account_id, prefix, counter)