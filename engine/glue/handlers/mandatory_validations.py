from pyspark.sql.functions import lit, col, when, concat, trim, udf, length

from .base_step import Step
from .observability import StepMetric
import json
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as f
from enum import Enum

@Step(
    type="mandatory_validations",
    props_schema={
        "type" : "object",
        "properties" : {
            "target" : {"type" : "string"},
            "validation_rule" : {"type" : "string"},
            "columns" : {"type" : "string"},
            "mandatory_columns" : {
                "type" : "object",
                "properties" : {
                    "columns" : {"type" : "string"}
                },
                "required" : ["columns"]
            }
        }
    }
)
class MandatoryValidations :

    def mandatory_columns_check(self, df, mandatory_columns) :

        """'
        perform row level check to ensure that the value is populated for the given field
        If there is null values, create a column - "system_error_message" or "error_message"
        """
        self.logger.info('check_null_values started')
        # df.show()
        for colName in mandatory_columns :
            if colName != "" :
                print("colName: ", colName)
                df = df.withColumn("system_error_message",
                                   when(df[colName].isNull() | (trim(col(colName)) == ''),
                                        concat(df['system_error_message'], lit(", "), lit(colName),
                                               lit(' is mandatory')))
                                   .otherwise(df['system_error_message']))  # concat(lit(colName), lit(','), df['error'])
        # df.show()
        self.logger.info('mandatory validation completed')
        return df

    def run_step(self, spark, config, context=None, glueContext=None) :
        print("starting mandatory validation handler")
        job_name = config.args.get('JOB_NAME')
        df = context.df(self.props.get("target"))
        validation_rule = self.props.get("validation_rule")
        target = self.props.get("target")
        self.logger.info(f"READING target: {target} ")
        df_bad = None
        if validation_rule == ValidationRule.MANDATORY_COLUMNS_CHECK.value :
            df = self.mandatory_columns_check(df, self.props.get("mandatory_columns").get("columns").split(','))
        else :
            raise ValueError('Specify atleast one validation rule')

        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("mandatory validation end")

    def register_dataframe(self, context, df, suffix="") :
        if df is not None :
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)


class ValidationRule(Enum) :
    MANDATORY_COLUMNS_CHECK = "mandatory_columns_check"