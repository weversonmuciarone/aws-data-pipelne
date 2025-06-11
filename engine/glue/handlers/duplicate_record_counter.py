import string
from typing import cast

from pyspark.sql.functions import substring_index, input_file_name
from pyspark.sql.types import StringType

from .base_step import Step, StepMetric
from jinja2 import Template
from pyspark.sql import functions as f


@Step(
    type="dup_record_counter",
    props_schema={
        "type" : "object",
        "properties" : {
            "target" : {"type" : "string"},
            "columns_to_include" : {
                "type" : "array",
                "items" : {"type" : "string"}
            }
        },
        "required" : ["target", "columns_to_include"]
    }
)
class DuplicateRecordCounter :
    def run_step(self, spark, config, context=None, glueContext=None) :
        print("duplicate record counter handler starting")
        job_name = config.args.get('JOB_NAME')
        target = self.props.get('target')
        columns_to_include = self.props.get('columns_to_include')

        df = context.df(target)
        print("count")
        # print(df.count())
        error_column = config.get_variable('error_column')
        if error_column is None:
            raise Exception("Variable error_column is undefined!")

        df_dup_records = df.groupBy(columns_to_include).count()
        df_dup_records_renamed = df_dup_records.select([f.col(c).alias(c + "_dup") for c in df_dup_records.columns])
        join_string = ''
        final_string = ''
        for col_name in columns_to_include :
            join_string = " df." + col_name + " = df_dup_records_renamed." + col_name + "_dup AND"
            final_string = final_string + join_string

        join_string = final_string[1 :-4]
        df.createOrReplaceTempView("df")
        df_dup_records_renamed.createOrReplaceTempView("df_dup_records_renamed")
        new_df = spark.sql(
            "SELECT df.*, df_dup_records_renamed.count_dup FROM df JOIN df_dup_records_renamed ON " + join_string)  # df1.id = df2.id")
        # new_df.show()
        df = new_df.withColumn(error_column,
                               f.when((f.col('count_dup') > 1),
                                      f.when(f.length(error_column) == 0, (f'This row has duplicate'))
                                      .otherwise(f', This row has duplicate')).
                               otherwise(f.lit('')))
        # print(df.count())
        self.register_dataframe(context, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("duplicate record counter handler ended")

    def register_dataframe(self, context, df, suffix="") :
        if df is not None :
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)