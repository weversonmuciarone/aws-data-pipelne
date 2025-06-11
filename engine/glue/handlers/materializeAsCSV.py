from .base_step import Step
from .observability import StepMetric


# import boto3


@Step(
    type="csv_materialize",
    props_schema={
        "type": "object",
        "required": [
            "bucket", "prefix", "from"
        ],
        "properties": {
            "target": {"type": "string"},
            "path": {"type": "string"},
            "bucket": {"type": "string"},
            "prefix": {"type": "string"},
            "mode": {"type": "string"},
            "description": {"type": "string"},
            "options": {
                "type": "object",
                "properties": {
                    "format": {"type": "string"},
                    "header": {"type": "boolean"},
                    "delimiter": {"type": "string"}
                },
                "required": ["format","delimiter"],
            }
        },
        "required": ["target", "options"],
        "oneOf": [
            {"required": ["path"]},
            {"required": ["bucket", "prefix"]}
        ]

    }
)
class Save:
    def run_step(self, spark, config=None, context=None, glueContext=None):
        print("csv materialize handler starting")
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
        # df.show()
        # if (len(df.head(1)) == 0) | (df.count() == 0) :
        #     self.logger.error("DataFrame should not be empty while materializing")
        #     print()
        #     raise Exception("Empty dataframe received for materializing")

        if df.filter(df["system_error_message"] != "").count() > 0:
            unique_filename = [row.filename for row in df.select('filename').distinct().collect()]
            for file in unique_filename:
                df_bad = df.filter(df['filename'] == file)
                df_bad = df_bad.drop('filename')
                df_bad.repartition(1).write \
                    .mode(write_mode) \
                    .option("header", self.props.get("header", True)) \
                    .option("delimiter", self.props.get("delimiter",';')) \
                    .csv(path + '/' + file)
            self.logger.error("Uploaded file have errors \n")
            print()
            raise Exception("Uploaded file have errors")
        else:
            df = df.drop("system_error_message")
            # context.register_df(self.name, df)
            self.register_dataframe(context, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("csv materialize handler ended")

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)