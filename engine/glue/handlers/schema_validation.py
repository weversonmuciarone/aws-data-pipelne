from .base_step import Step
import pathlib
from jinja2 import Template
from .observability import StepMetric
import copy
import collections
from pyspark.sql import functions as f
import boto3
import urllib.parse as up

@Step(
    type="schema_validation",
    props_schema={
        "type":  "object",
        "properties":{
            "target" : {"type": "string"},
            "schema_syntax" : {"type": "string"},
            "schema_res" : {"type": "string"},
            "post_validation": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "config": {"type": "object"}
                },
                "required": ["name", "config"]
            }
        },
        "required": ["target", "schema_syntax", "schema_res"]
    }
)
class SchemaValidation:
    def run_step(self, spark, config, context=None, glueContext= None):
        print("schema validation handler starting")
        job_name = config.args.get('JOB_NAME')
        prefix=f"{self.name} [{self.type}]"

        schema_res = self.props.get("schema_res")
        target = self.props.get("target")
        schema_syntax = self.props.get("schema_syntax")
        opt_post_validate = self.props.get('post_validation', {'name': None, 'config': {}})
        post_validate_strategy = PostValidationStrategy.provider[opt_post_validate['name']](
                logger=self.logger,
                **opt_post_validate['config']
                )
        df_under_test = context.df(target)

        if schema_syntax == "csv":
            '''
            We expect a file containing a single line of comma-separated column names.
            Eg: col_01,col_02
            '''
            df_with_required_schema = spark.read.option("header", "true").csv(schema_res)
        elif schema_syntax == "ddl":
            '''
            We expect a file containing a single line of schema string in DDL-format.
            Eg: col0 INT, col1 DOUBLE
            '''
            schema_data = config.get_resource(schema_res)
            df_with_required_schema = spark.read.load(schema=schema_data)
        else:
            raise Exception(f"Unsupported syntax option {schema_syntax}.")

        current_schema = { field["name"]: field for field in df_under_test.schema.jsonValue()["fields"] }
        expected_schema = { field["name"]: field for field in df_with_required_schema.schema.jsonValue()["fields"] }

        self.logger.info(f"{prefix} RUNNING SCHEMA VALIDATION")
        self.logger.info(f"{prefix} Expected - {expected_schema}")
        self.logger.info(f"{prefix} Current - {current_schema}")

        for colname_e, coldata_e in expected_schema.items():
            exc_msg = None

            if colname_e not in current_schema:
                exc_msg = f"Column {colname_e} was not found in {target}."
                break

            coldata_c = current_schema[colname_e]
            if coldata_c != coldata_e:
                exc_msg = f"The provided type of {colname_e} ({coldata_c}) did not match the expected type {coldata_e}."
                break

        if exc_msg is not None:
            self.logger.error(f"{prefix} {exc_msg}")
            post_validate_strategy.fail(self.name, df_under_test, df_with_required_schema)
            raise Exception(exc_msg)

        df_under_test.createOrReplaceTempView(self.name)
        context.register_df(self.name, df_under_test)
        print("schema validation handler ended")


class PostValidationStrategy:

    provider = collections.defaultdict(lambda: NoOpPostValidationStrategy)

    def success(self, label_of_df, df_input, df_expected):
        pass

    def fail(self, label_of_df, df_input, df_expected):
        pass


class NoOpPostValidationStrategy(PostValidationStrategy):

    shortname = 'noop'

    def __init__(self, logger):
        self.logger = logger


class MoveFailedPostValidation(PostValidationStrategy):

    shortname = 'movefailed'

    def __init__(self, logger, bucket_fail, prefix_fail):
        self.logger = logger
        self.bucket_fail = bucket_fail
        self.prefix_fail = prefix_fail
        self.s3 = boto3.client('s3')

    def success(self, label_of_df, df_input, df_expected):
        return

    def fail(self, label_of_df, df_input, df_expected):
        failed_files = [r['fn'] for r in df_input.withColumn('fn', f.input_file_name()).select('fn').distinct().collect()]
        failed_file_count = len(failed_files)
        self.logger.info(f'Found {failed_file_count} {"file" if failed_file_count == 1 else "files"} backing the invalid dataset {label_of_df}.')

        for fl in failed_files:
            self.logger.info(fl)
            self.move(fl)

    def move(self, fl):
        '''
        fl is an s3 object url string. Eg: 's3://ingestion/plan_upload/latest.csv'
        '''
        parsed_url = up.urlparse(fl)
        bucket_src = parsed_url.netloc
        path_src = parsed_url.path
        key_src = path_src[1:]
        idx_last_sep = key_src.rfind('/')
        basename_src = key_src if idx_last_sep == -1 else key_src[idx_last_sep + 1:]
        try:
            self.s3.copy({'Bucket': bucket_src, 'Key': key_src}, self.bucket_fail, f'{self.prefix_fail}/{basename_src}')
            self.s3.delete_object(Bucket=bucket_src, Key=key_src)
        except Exception as e:
            self.logger.error(e)
            print(e)


PostValidationStrategy.provider.update({ cls.shortname: cls for cls in PostValidationStrategy.__subclasses__() })