import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType
from datetime import datetime
from pyspark.sql import SQLContext

import traceback
import sys

from .base_step import Step
from .observability import StepMetric


@Step(
    type="data_reconciliation",
    props_schema={
        "type": "object",
        "properties": {
            "compare_type": {"type": "string"},
            "source_location": {"type": "string"},
            "target_location": {"type": "string"},
        },
        "required": ["compare_type", "source_location", "target_location"]
    }
)
class ReconciliationComparison:

    def run_step(self, spark, config, context=None, glueContext=None):
        print("*********** Job Started ***********")
        # Read configuration parameters.
        job_name = config.args.get('JOB_NAME')

        # Getting required parameters.
        compare_type = self.props.get("compare_type")
        data_pipeline = str(config.args.get("JOB_NAME")).split(config.args.get("STAGE"))[0] + config.args.get("STAGE")
        glue_job = config.args.get('JOB_NAME')
        source_location_name = self.props.get("source_location")
        target_location_name = self.props.get("target_location")
        source_measure_columns = self.props.get("source_measure_columns", "")
        target_measure_columns = self.props.get("target_measure_columns", "")
        source_filter_sql = self.props.get("source_filter_sql", "")
        target_filter_sql = self.props.get("target_filter_sql", "")

        if '@' in source_location_name:
            input_type = 'dataframe'
        else:
            input_type = 'table'

        # Setting SQL Context.
        sql_context = SQLContext(spark.sparkContext)

        # Initializing count and final df to default values.
        source_df_count = 0
        target_df_count = 0
        source_df = 0
        target_df = 0
        df = None

        try:
            if compare_type == "count":
                if input_type == "dataframe":
                    # Get source and target details and split from special character to generate dataframe name and path.
                    source_location_str = self.props.get("source_location").split("@")
                    target_location_str = self.props.get("target_location").split("@")

                    # Convert input source and target into a dataframe.
                    source_location = context.df(source_location_str[0])
                    target_location = context.df(target_location_str[0])

                    if (target_filter_sql != "") or (source_filter_sql != ""):
                        source_location.registerTempTable("source_location_sql_df")
                        target_location.registerTempTable("target_location_sql_df")
                        source_location = sql_context.sql("select * from source_location_sql_df " + source_filter_sql)
                        target_location = sql_context.sql("select * from target_location_sql_df " + target_filter_sql)

                    # Separately keep input source and target path strings for metadata table creation.
                    source_location_name = source_location_str[1]
                    target_location_name = target_location_str[1]

                    # Get the count of two dataframes.
                    source_df_count = source_location.count()
                    target_df_count = target_location.count()
                elif input_type == "table":
                    if (target_filter_sql != "") or (source_filter_sql != ""):
                        source_df_count = spark.sql(
                            "SELECT * FROM " + self.props.get("source_location") + " " + source_filter_sql).count()
                        target_df_count = spark.sql(
                            "SELECT * FROM " + self.props.get("target_location") + " " + target_filter_sql).count()
                    else:
                        # Get the count of two tables.
                        source_df_count = spark.sql("SELECT * FROM " + self.props.get("source_location")).count()
                        target_df_count = spark.sql("SELECT * FROM " + self.props.get("target_location")).count()

                # Round float values to 2 decimal places.
                if isinstance(source_df_count, float):
                    source_df_count = round(source_df_count, 2)
                if isinstance(target_df_count, float):
                    target_df_count = round(target_df_count, 2)

                # Generic Condition to check the row count of 2 dataframes.
                if source_df_count == target_df_count:
                    match_flag = "Y"
                else:
                    match_flag = "N"

                # Create meta data table with count information.
                columns = ["pipeline_name", "glue_name", "source_location", "target_location", "source_count",
                           "target_count", "measure_column", "match_flag", "notification_sent"]
                data = [(data_pipeline, glue_job, source_location_name, target_location_name, str(source_df_count),
                         str(target_df_count), "", match_flag, "N")]

                df = spark.createDataFrame(data, columns)

            elif compare_type == "measure":
                if input_type == "table":
                    if (target_filter_sql != "") or (source_filter_sql != ""):
                        source_df = spark.sql(
                            "SELECT * FROM " + self.props.get("source_location") + " " + source_filter_sql)
                        target_df = spark.sql(
                            "SELECT * FROM " + self.props.get("target_location") + " " + target_filter_sql)
                    else:
                        source_df = spark.sql("SELECT * FROM " + self.props.get("source_location"))
                        target_df = spark.sql("SELECT * FROM " + self.props.get("target_location"))
                elif input_type == "dataframe":
                    # Get source and target as a dataframes.
                    source_location_str = self.props.get("source_location").split("@")
                    target_location_str = self.props.get("target_location").split("@")

                    source_df = context.df(source_location_str[0])
                    target_df = context.df(target_location_str[0])

                    if (target_filter_sql != "") or (source_filter_sql != ""):
                        source_df.registerTempTable("source_location_sql_df")
                        target_df.registerTempTable("target_location_sql_df")
                        source_df = sql_context.sql("select * from source_location_sql_df " + source_filter_sql)
                        target_df = sql_context.sql("select * from target_location_sql_df " + target_filter_sql)

                    source_location_name = source_location_str[1]
                    target_location_name = target_location_str[1]

                columns = ["pipeline_name", "glue_name", "source_location", "target_location", "source_count",
                           "target_count", "measure_column", "match_flag", "notification_sent"]
                df = None

                for measure_col_s, measure_col_t in zip(source_measure_columns.split(","),
                                                        target_measure_columns.split(",")):
                    source_count = source_df.select(f.sum(f.col(measure_col_s))).first()[0]
                    target_count = target_df.select(f.sum(f.col(measure_col_t))).first()[0]

                    # Round float values to 2 decimal places.
                    if isinstance(source_count, float):
                        source_count = round(source_count, 2)
                    if isinstance(target_count, float):
                        target_count = round(target_count, 2)

                    # Matching source count and target count.
                    if source_count == target_count:
                        match_flag = "Y"
                    else:
                        match_flag = "N"

                    data = [(data_pipeline, glue_job, source_location_name, target_location_name, str(source_count),
                             str(target_count), measure_col_s + "|" + measure_col_t, match_flag, "N")]

                    if df is None:
                        df = spark.createDataFrame(data, columns)
                    else:
                        df = spark.createDataFrame(data, columns)
                        df = df.union(df)

            else:
                raise Exception("Input compare type is not matching: " + compare_type)

            # Add current run date as a column.
            df = df.withColumn('reconc_ts', f.lit(datetime.now()).cast(TimestampType()))

        except Exception as e:
            print(e)
            traceback.print_exception(type(e), e, e.__traceback__, file=sys.stdout)

            # Register the dataframe.
        df.createOrReplaceTempView(self.name)
        context.register_df(self.name, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
