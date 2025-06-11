from .base_step import Step
from .observability import StepMetric
from pyspark.sql import functions as f
from enum import Enum


@Step(
    type="validate",
    props_schema={
        "type":  "object",
        "properties": {
            "target": {"type": "string"},
            "validation": {"type": "string"},
            "length": {"type": "integer"},
            "columns": {"type": "string"},
            "date_range": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "columns": {"type": "string"}
                }
            }
        },
        "required": ["validation", "columns"]
    }
)
class Validation:

    def truncate_column_values(self, df, length, columns_to_be_truncated):
        if columns_to_be_truncated == "*":
            columns_to_be_truncated = df.columns
        else:
            columns_to_be_truncated = columns_to_be_truncated.split(",")
        for col_name in columns_to_be_truncated:
            print(col_name)
            df = df.withColumn(col_name, f.col(col_name).substr(0, length))
        return df

    def date_range_filter(self, df, date_from, date_to, col_name):
        if date_from == "" or date_to == "":
            return None
        filtered = df.where(f.col(col_name).between(date_from, date_to))
        bad = df.exceptAll(filtered)
        return filtered, bad

    def run_step(self, spark, config, context=None, glueContext=None):
        job_name = config.args.get('JOB_NAME')
        df = context.df(self.props.get("target"))
        length = self.props.get("length", 10000)
        columns = self.props.get("columns", "*")
        validation_type = self.props.get("validation")
        df_bad = None
        if validation_type == ValidationType.TRUNCATE_COLUMN_VALUES.value:
            df = self.truncate_column_values(df, length, columns)
        elif validation_type == ValidationType.DATE_RANGE.value:
            date_from = self.props.get("date_range").get("date_from")
            date_to = self.props.get("date_range").get("date_to")
            date_columns = self.props.get("date_range").get("columns", "*")
            df, df_bad = self.date_range_filter(df, date_from, date_to, date_columns)
        elif validation_type == ValidationType.ALL.value:
            print("truncating")
            df = self.truncate_column_values(df, length, columns)
            # date_from = self.props.get("date_range").get("date_from")
            # date_to = self.props.get("date_range").get("date_to")
            # date_columns = self.props.get("date_range").get("columns", "*")
            # df = self.date_range_filter(df, date_from, date_to, date_columns)
        else:
            raise ValueError('Specify atleast one validation type')
        target = self.props.get("target")
        df.show()
        self.logger.info(f"READING target: {target} ")
        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count",unit="NbRecord",value=df.rdd.countApprox(timeout = 800, confidence = 0.5)))

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name+suffix)
            context.register_df(self.name+suffix, df)


class ValidationType(Enum):
    TRUNCATE_COLUMN_VALUES = "truncate_column_values"
    DATE_RANGE = "date_range"
    ALL = "all"
