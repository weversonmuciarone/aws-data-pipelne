import re
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, DoubleType

import datetime
import random
import boto3

from .base_step import Step
from .observability import StepMetric


@Step(
    type="data_anonymization",
    props_schema={
        "type": "object",
        "properties": {
            "anonymize_cols": {"anonymize_cols": "string"},
            "source_prefix": {"source_prefix": "string"},
            "source_bucket": {"source_bucket": "string"},
            "target_prefix": {"target_prefix": "string"},
            "target_bucket": {"target_bucket": "string"},
            "file_format": {"file_format": "string"},
            "conversion_type": {"file_format": "string"},
        },
        "required": ["anonymize_cols", "source_bucket", "source_prefix", "target_prefix", "target_bucket",
                     "file_format", "conversion_type"]
    }
)
class DataAnonymization:
    def save_df_to_s3(self, need_bucket, file_path_to_save, file_format, df):
        path = "s3://" + need_bucket + "/" + file_path_to_save
        s3_r = boto3.resource('s3')
        s3 = boto3.client('s3')

        if file_format == 'parquet':
            df.write.mode('overwrite').parquet(path)
        else:
            df.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').option("header",
                                                                                                "true").option(
                "ignoreLeadingWhiteSpace", False).option("ignoreTrailingWhiteSpace", False).save(path)

        acl_enabled = True
        try:
            s3_client = boto3.client('s3', region_name='eu-west-1')
            response = s3_client.get_bucket_ownership_controls(
                Bucket=need_bucket
            )
            if "BucketOwnerEnforced" in str(response):
                acl_enabled = False
        except:
            acl_enabled = True
            print("ACL enabled")

        if acl_enabled:
            all_objects = s3.list_objects(Bucket=need_bucket, Prefix=file_path_to_save)
            objects_list = all_objects.get('Contents')
            for item in range(len(objects_list)):
                object_name_with_path = objects_list[item].get('Key')
                # actual_object_name = file_path_to_save + object_name_with_path
                s3_r.Object(need_bucket, object_name_with_path).Acl().put(ACL='bucket-owner-full-control')

    def read_s3_data(self, spark, bucket, prefix, file_format):
        path = f's3://{bucket}/{prefix}'

        if file_format == 'csv':
            return spark.read.options(header='True').format("csv").load(path)
        if file_format == 'parquet':
            return spark.read.options(header='True').format("parquet").load(path)

    def run_step(self, spark, config, context=None, glueContext=None):
        print("*********** Job Started ***********")
        job_name = config.args.get('JOB_NAME')
        source_prefix = self.props.get("source_prefix")
        source_bucket = self.props.get("source_bucket")
        target_prefix = self.props.get("target_prefix")
        target_bucket = self.props.get("target_bucket")
        file_format = self.props.get("file_format")
        conversion_type = self.props.get("conversion_type")

        # Read input file into a CSV
        target: DataFrame = self.read_s3_data(spark, source_bucket, source_prefix, file_format)

        # Get the column details need to anonymize.
        anonymize_cols = self.props.get("anonymize_cols").split(",")

        # Method to anonymize date by incrementing sum of days.
        @f.udf(returnType=StringType())
        def anonymize_date_udf(col_value, d_format):
            split_char = col_value[4]
            date_split = str(col_value).split(split_char)
            sum_date_array = sum(list(map(int, date_split)))
            date_1 = datetime.datetime.strptime(str(col_value), d_format)
            anonymize_date = date_1 + datetime.timedelta(days=int(sum_date_array))

            if d_format != "%Y-%m-%d":
                anonymize_date = anonymize_date.strftime(d_format)
            else:
                anonymize_date = anonymize_date.strftime(d_format)
            return anonymize_date

        @f.udf(returnType=StringType())
        def anonymize_datetime_udf(col_value, dt_format):
            date_1 = datetime.datetime.strptime(col_value, dt_format)
            split_char = str(date_1.date())[4]
            date_split = str(date_1.date()).split(split_char)
            sum_date_array = sum(list(map(int, date_split)))
            anonymize_date = date_1 + datetime.timedelta(days=int(sum_date_array))
            if dt_format != "%Y-%m-%d %H:%M:%S":
                anonymize_date = anonymize_date.strftime(dt_format)
            else:
                anonymize_date = anonymize_date.strftime("%Y-%m-%d %H:%M:%S")
            return anonymize_date

        @f.udf(returnType=StringType())
        def anonymize_string_udf(col_value):
            col_value = str(col_value).lower()
            dic_alphabet = {"a": "n", "n": "a", "b": "m", "m": "b", "c": "l", "l": "c", "d": "k", "k": "d", "e": "j", "j": "e", "f": "i", "i": "f", "g": "h", "h": "g", "o": "z", "z": "o", "p": "y", "y": "p", "q": "x", "x": "q", "r": "w", "w": "r", "s": "v", "v": "s", "t": "u", "u": "t", "1": "4", "4": "1", "2": "5", "5": "2", "3": "6", "6": "3", "7": "9", "9": "7", "0": "8", "8": "0"}

            final_value = ""
            for i in col_value:
                if i.isalnum():
                    final_value = final_value + i.replace(i, dic_alphabet[i])
                else:
                    final_value = final_value + i

                if final_value[0] == "0":
                    final_value = final_value.replace("0", "7")
            return final_value

        # Common UDF function to anonymize data.
        @f.udf(returnType=StringType())
        def common_mask_col_udf(col_value, replace_char):
            anonymize_str = ''
            for c in str(col_value):
                if type(replace_char) == int:
                    anonymize_str = anonymize_str + re.sub("[0-9a-zA-Z]+", str(random.randint(0, 9)), c)
                else:
                    anonymize_str = anonymize_str + re.sub("[0-9a-zA-Z]+", str(replace_char), c)
            return str(anonymize_str)

        @f.udf(returnType=IntegerType())
        def mask_int_col_udf(col_value):
            n = len(str(col_value))
            range_start = 10 ** (n - 1)
            range_end = (10 ** n) - 1
            return random.randint(range_start, range_end)

        @f.udf(returnType=StringType())
        def format_date(d_type, dd_format):
            if d_type == "date":
                date_1 = datetime.datetime.strptime('1900-01-01', "%Y-%m-%d").strftime(dd_format)
            elif d_type == "datetime":
                date_1 = datetime.datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S").strftime(dd_format)
            else:
                date_1 = '1900-01-01'
            return str(date_1)

        date_format = "%Y-%m-%d"
        date_time_format = "%Y-%m-%d %H:%M:%S"

        # Iterate through each column to anonymize the data in it.
        for anonymize_col in anonymize_cols:
            try:
                split_val = anonymize_col.split("|")
                col_name = split_val[0]
                col_type = split_val[1]

                if conversion_type == "mask":
                    if col_type == "date":
                        if len(split_val) == 3:
                            date_format = split_val[2]
                        if date_format == "%Y-%m-%d":
                            target = target.withColumn(col_name,
                                                       format_date(f.lit("date"), f.lit(date_format)).cast(DateType()))
                        else:
                            target = target.withColumn(col_name, format_date(f.lit("date"), f.lit(date_format)).cast(
                                StringType()))
                    elif col_type == "datetime":
                        if len(split_val) == 3:
                            date_time_format = split_val[2]
                        if date_time_format == "%Y-%m-%d %H:%M:%S":
                            target = target.withColumn(col_name,
                                                       format_date(f.lit("datetime"), f.lit(date_time_format)).cast(
                                                           TimestampType()))
                        else:
                            target = target.withColumn(col_name,
                                                       format_date(f.lit("datetime"), f.lit(date_time_format)).cast(
                                                           StringType()))
                    elif col_type == "int":
                        target = target.withColumn(col_name, mask_int_col_udf(f.col(col_name)))
                    elif col_type == "decimal":
                        target = target.withColumn(col_name,
                                                   common_mask_col_udf(f.col(col_name), f.lit(1)).cast(DoubleType()))
                    else:
                        target = target.withColumn(col_name,
                                                   common_mask_col_udf(f.col(col_name), f.lit('x')).cast(StringType()))

                elif conversion_type == "anonymize":
                    if col_type == "date":
                        if len(split_val) == 3:
                            date_format = split_val[2]
                        if date_format == "%Y-%m-%d":
                            target = target.withColumn(col_name,
                                                       anonymize_date_udf(f.col(col_name), f.lit(date_format)).cast(
                                                           DateType()))
                        else:
                            target = target.withColumn(col_name,
                                                       anonymize_date_udf(f.col(col_name), f.lit(date_format)).cast(
                                                           StringType()))
                    elif col_type == "datetime":
                        if len(split_val) == 3:
                            date_time_format = split_val[2]
                        if date_time_format == "%Y-%m-%d %H:%M:%S":
                            target = target.withColumn(col_name, anonymize_datetime_udf(f.col(col_name),
                                                                                        f.lit(date_time_format)).cast(
                                TimestampType()))
                        else:
                            target = target.withColumn(col_name, anonymize_datetime_udf(f.col(col_name),
                                                                                        f.lit(date_time_format)).cast(
                                StringType()))
                    elif col_type == "int":
                        target = target.withColumn(col_name, anonymize_string_udf(f.col(col_name)).cast(IntegerType()))
                    elif col_type == "decimal":
                        target = target.withColumn(col_name, anonymize_string_udf(f.col(col_name)).cast(DoubleType()))
                    else:
                        target = target.withColumn(col_name, anonymize_string_udf(f.col(col_name)).cast(StringType()))

            except Exception as e:
                print("Failed to process the column: " + anonymize_col)
                print(e)

        # Write anonymize dataframe back to CSV.
        self.save_df_to_s3(target_bucket, target_prefix, file_format, target)

        # Register to anonymize dataframe.
        target.createOrReplaceTempView(self.name)
        context.register_df(self.name, target)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=target.rdd.countApprox(timeout=800, confidence=0.5)))
