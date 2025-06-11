from datetime import datetime
import pyspark.sql.functions as f

from .base_step import Step
from .observability import StepMetric

import boto3
import json


@Step(
    type="table_notification",
    props_schema={
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "bucket": {"type": "string"},
            "prefix": {"type": "string"},
            "central_log_bucket": {"type": "string"}
        },
        "required": ["table", "bucket", "prefix", "central_log_bucket"]
    }
)
class ReconciliationNotification:

    def run_step(self, spark, config, context=None, glueContext=None):
        print("*********** Job Started ***********")

        # Read configuration parameters.
        job_name = config.args.get('JOB_NAME')
        pipeline_name = str(config.args.get("JOB_NAME")).split(config.args.get("STAGE"))[0] + config.args.get("STAGE")
        table = self.props.get("table")
        bucket = self.props.get("bucket")
        prefix = self.props.get("prefix")
        central_log_bucket = self.props.get("central_log_bucket")

        timestamp = str(datetime.now())
        guid = config.args.get('JOB_RUN_ID')

        # Read metadata table.
        df = spark.sql("SELECT * FROM " + table)

        # Filter data based on pipeline and notification flag that need to be emailed.
        df_notification = df.filter((f.col("pipeline_name") == pipeline_name) & (f.col("notification_sent") == "N"))

        # Subtract metadata df from notification df to get the records that will not sent to user.
        df_rest = df.subtract(df_notification)

        # Convert the filtered notification df to html table in string format.
        table_html = df_notification.drop(f.col('notification_sent')).toPandas().to_html(index=False)

        # Create notification JSON object.
        notification_log_info = {
            "timestamp": timestamp,
            "guid": guid,
            "process": str(config.args.get("JOB_NAME")).split(config.args.get("STAGE"))[0] + config.args.get("STAGE"),
            "job": "",
            "step": "",
            "eventType": "PipelineEnd",
            "status": "SUCCESS",
            "msg": table_html
        }

        # Save JSON object in S3 central logging bucket.
        sf_name = str(config.args.get("JOB_NAME")).split(config.args.get("STAGE"))[0] + config.args.get("STAGE")
        email_notification_path = "central_logs/logs"
        file_name = timestamp + "_" + sf_name + ".json"
        path = f"{email_notification_path}/{file_name}"

        s3_client = boto3.client('s3', region_name='eu-west-1')
        s3_client.put_object(Body=str(json.dumps(notification_log_info)), Bucket=central_log_bucket, Key=path)

        # Update the notification flag to "Y".
        df_notification = df_notification.withColumn('notification_sent', f.lit("Y"))

        # Concat two dataframes to generate the full metadata table with updated notification flag.
        updated_df = df_notification.unionByName(df_rest)

        spark.catalog.refreshTable(table)
        updated_df.cache()
        updated_df.show()

        # Overwrite data in metadata table with updated records.
        updated_df.write.mode('overwrite').parquet("s3://" + bucket + "/" + prefix)

        # Register the dataframe.
        df_notification.createOrReplaceTempView(self.name)
        context.register_df(self.name, df_notification)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df_notification.rdd.countApprox(timeout=800, confidence=0.5)))
