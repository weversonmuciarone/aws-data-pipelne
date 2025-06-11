from .base_step import Step
from .observability import StepMetric
from datetime import datetime
import time
from pyspark.sql.functions import lit, unix_timestamp

import boto3


@Step(
    type="materialize_hive",
    props_schema={
        "type": "object",
        "required": [
            "bucket", "prefix", "database", "table", "from"
        ],
        "properties": {
            "target": {"type": "string"},
            "path": {"type": "string"},
            "bucket": {"type": "string"},
            "prefix": {"type": "string"},
            "database": {"type": "string"},
            "table": {"type": "string"},
            "mode": {"type": "string"},
            "extract": {"type": "string"},
            "description": {"type": "string"},
            "options": {
                "type": "object",
                "properties": {
                    "format": {"type": "string"},
                    "header": {"type": "boolean"},
                    "delimiter": {"type": "string"},
                    "partitions": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["format"],
            }
        },
        "required": ["database", "table", "target", "options"],
        "oneOf": [
            {"required": ["path"]},
            {"required": ["bucket", "prefix"]}
        ]

    }
)
class MaterializeHive:
    def run_step(self, spark, config=None, context=None, glueContext=None):
        prefix = f"{self.name} [{self.type}]"
        job_name = config.args.get("JOB_NAME")

        df = context.df(self.props.get("target"))
        ref = context.ref(self)
        ref(self.props.get("target"))
        path = ""
        if self.props.get("bucket"):
            path = f"s3://{self.props.get('bucket')}/{self.props.get('prefix')}"
            print("path: ", path)
        elif self.props.get("path"):
            path = self.props.get("path")
        write_mode = self.props.get("mode", "overwrite")

        dbname = self.props.get('database')
        if dbname == "":
            table_name = self.props.get('table')
        else:
            table_name = f"{dbname}.{self.props.get('table')}"

        write_mode = self.props.get("mode", "overwrite")
        writer =  df \
                 .write \
                 .mode(write_mode) \
                 .format(self.props.get("options",{}).get("format","parquet")) \
                 .option("path", path) \
                 .option("header", self.props.get("header", True)) \
                 .option("delimiter", self.props.get("delimiter", ","))
        partitions = self.props.get("options",{}).get("partitions")
        if partitions:
            self.logger.info("Partitions by {}".format(str(partitions)))
            writer = writer.partitionBy(partitions)

        if df.count() > 0 :
            writer.saveAsTable(table_name, mode = write_mode)

        self.register_dataframe(context, df)

        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)