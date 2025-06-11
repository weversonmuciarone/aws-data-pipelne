from time import sleep

from .base_step import Step
import pathlib
from jinja2 import Template
from .observability import StepMetric
import copy
import boto3


@Step(
    type="athena_query",
    props_schema={
        "type" : "object",
        "properties" : {
            "sql" : {"type" : "string"},
            "file" : {"type" : "string"}
        },
        "oneOf" : [
            {"required" : ["file"]},
            {"required" : ["sql"]}
        ]
    }
)
class Query :
    def __get_query_status_response(self, query_execution_id) :
        response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        return response

    def wait_for_query_to_complete(self, query_execution_id):
        is_query_running = True
        backoff_time = 10
        while is_query_running:
            response = self.__get_query_status_response(query_execution_id)
            status = response["QueryExecution"]["Status"]["State"]  # possible responses: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            print("status: ")
            print(status)
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
        # bucket = self.props.get("bucket")
        prefix = self.props.get("prefix")
        # table = self.props.get("table")
        # database = self.props.get("database")

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


        '''
        query to create table
        '''
        response = athena.start_query_execution(
            QueryString=processed
            # QueryExetionContext={"Database": database}
            # ResultConfiguration={'OutputLocation': path}
        )

        query_execution_id = response["QueryExecutionId"]
        self.wait_for_query_to_complete(query_execution_id)
        print("response")
        print(response)
        print('Done.')