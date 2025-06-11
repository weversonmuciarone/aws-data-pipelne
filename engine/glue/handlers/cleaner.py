from time import sleep

from .base_step import Step
import pathlib
from jinja2 import Template
from .observability import StepMetric
import copy
import boto3


@Step(
    type="athena_cleaner",
    props_schema={
        "type" : "object",
        "properties" : {
            "sql" : {"type" : "string"},
            "file" : {"type" : "string"},
            "bucket" : {"type" : "string"},
            "prefix" : {"type" : "string"},
            "database" : {"type" : "string"},
            "table": {"type" : "string"}
        },
        "oneOf" : [
            {"required" : ["file"]},
            {"required" : ["sql"]}
        ]
    }
)
class Cleaner :
    def __get_query_status_response(self, query_execution_id) :
        response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        return response

    def wait_for_query_to_complete(self, query_execution_id):
        is_query_running = True
        backoff_time = 10
        while is_query_running:
            response = self.__get_query_status_response(query_execution_id)
            status = response["QueryExecution"]["Status"]["State"]  # possible responses: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            if status == "SUCCEEDED":
                print("query succeeded")
                is_query_running = False
            elif status in ["CANCELED", "FAILED"]:
                print("query failed")
                raise Exception("athena query failed or canceled!")
            elif status in ["QUEUED", "RUNNING"]:
                print("running")
                sleep(backoff_time)

    def run_step(self, spark, config, context=None, glueContext=None) :
        athena = boto3.client('athena')
        self.client = athena
        job_name = config.args.get('JOB_NAME')
        prefix = f"{self.name} [{self.type}]"
        bucket = self.props.get("bucket")
        prefix = self.props.get("prefix")
        database = self.props.get("database")
        table = self.props.get("table")
        '''
        delete already existing data in s3
        '''
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' in response :
            for object in response['Contents'] :
                print('Deleting', object['Key'])
                s3_client.delete_object(Bucket=bucket, Key=object['Key'])
            print("files deleted")

        '''
        delete tables from athena
        '''
        if self.props.get("sql") :
            self.logger.info("{prefix} READING INLINE SQL")
            query = self.props.get("sql")

        elif self.props.get("file") :
            self.logger.info("{prefix} READING SQL FILE")
            query = config.get_query(self.props.get("file"))

        self.logger.info("{prefix} RESOLVING TEMPLATE")

        rendering_vars = config.variables

        template = Template(query)
        processed = template.render(rendering_vars)
        print("Query is:")
        print(processed)
        self.logger.info("{prefix} RESOLVED TEMPLATE")
        self.logger.info(f"{prefix} RUNNING QUERY {processed}")

        glue_client = boto3.client('glue', region_name='eu-west-1')
        responseGetTables = glue_client.get_tables(DatabaseName=database)
        tableList = responseGetTables['TableList']

        for tableDict in tableList :
            tableName = tableDict['Name']
            if tableName == table:
                delete_response = athena.start_query_execution(
                    QueryString=processed
                )

                query_execution_id = delete_response["QueryExecutionId"]
                self.wait_for_query_to_complete(query_execution_id)
                print("delete_response")
                print(delete_response)
            else:
                print("its different table")