from pyspark.sql.functions import lit, col, when, concat, trim, udf

from .base_step import Step
from .observability import StepMetric
import json
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as f
from enum import Enum


def concat_values(amount_period_1, amount_period_2, amount_period_3, amount_period_4, amount_period_5,
                  amount_period_6, amount_period_7, amount_period_8,
                  amount_period_9, amount_period_10, amount_period_11, amount_period_12, mandatory_columns,
                  system_error_message) :
    err = ''
    print(type(mandatory_columns))

    if isinstance(mandatory_columns, str) :
        mandatory_columns_list = mandatory_columns.split(",")

        if "amount_period_1" in mandatory_columns_list :
            if amount_period_1 == '' or amount_period_1 is None :
                err = err + " amount_period_1 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '':
                #     err = err + " amount_period_1 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_1 is mandatory for this plan type"

        if "amount_period_2" in mandatory_columns_list :
            if amount_period_2 == '' or amount_period_2 is None :
                err = err + " amount_period_2 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_2 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_2 is mandatory for this plan type"

        if "amount_period_3" in mandatory_columns_list :
            if amount_period_3 == '' or amount_period_3 is None :
                err = err + " amount_period_3 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_3 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_3 is mandatory for this plan type"

        if "amount_period_4" in mandatory_columns_list :
            if amount_period_4 == '' or amount_period_4 is None :
                err = err + " amount_period_4 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_4 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_4 is mandatory for this plan type"

        if "amount_period_5" in mandatory_columns_list :
            if amount_period_5 == '' or amount_period_5 is None :
                err = err + " amount_period_5 is mandatory for this plan type"
                # if (system_error_message is None) or trim(system_error_message) == '' :
                #     err = err + " amount_period_5 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_5 is mandatory for this plan type"

        if "amount_period_6" in mandatory_columns_list :
            if amount_period_6 == '' or amount_period_6 is None :
                err = err + " amount_period_6 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_6 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_6 is mandatory for this plan type"

        if "amount_period_7" in mandatory_columns_list :
            if amount_period_7 == '' or amount_period_7 is None :
                err = err + " amount_period_7 is mandatory for this plan type"
                # if (system_error_message is None) or trim(system_error_message) == '' :
                #     err = err + " amount_period_7 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_7 is mandatory for this plan type"

        if "amount_period_8" in mandatory_columns_list :
            if amount_period_8 == '' or amount_period_8 is None :
                err = err + " amount_period_8 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_8 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_8 is mandatory for this plan type"

        if "amount_period_9" in mandatory_columns_list :
            if amount_period_9 == '' or amount_period_9 is None :
                err = err + " amount_period_9 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_9 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_9 is mandatory for this plan type"

        if "amount_period_10" in mandatory_columns_list :
            if amount_period_10 == '' or amount_period_10 is None :
                err = err + " amount_period_10 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_10 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_10 is mandatory for this plan type"

        if "amount_period_11" in mandatory_columns_list :
            if amount_period_11 == '' or amount_period_11 is None :
                err = err + " amount_period_11 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_11 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_11 is mandatory for this plan type"

        if "amount_period_12" in mandatory_columns_list :
            if amount_period_12 == '' or amount_period_12 is None :
                err = err + " amount_period_12 is mandatory for this plan type"
                # if system_error_message is None or trim(system_error_message) == '' :
                #     err = err + " amount_period_12 is mandatory for this plan type"
                # else:
                #     err = err + ", amount_period_ is mandatory for this plan type"
    return err


@Step(
    type="mandatory_if_validations",
    props_schema={
        "type" : "object",
        "properties" : {
            "target" : {"type" : "string"},
            "target2" : {"type" : "string"},
            "validation_rule" : {"type" : "string"},
            "columns" : {"type" : "string"},
            "check_mandatory_columns" : {
                "type" : "object",
                "properties" : {
                    "columns" : {"type" : "string"}
                },
                "required" : ["columns"]
            },
            "mandatory_if_columns" : {
                "type" : "object",
                "properties" : {
                    "columns" : {"type" : "string"}
                    # "mandatory_if_cols": {"type": "string"}
                },
                "required" : ["columns"]
            },
            "mapping" : {"type" : "string"},
            "max_column_limit" : {"type" : "integer"}
        }
    }
)
class MandatoryIfValidations :

    def mandatory_if_check(self, df, df2, conditional_columns) :  # , mandatory_if_columns):
        self.logger.info('mandatory_if started')
        df = df.join(df2, on=conditional_columns, how='left')

        concat_values_udf = udf(concat_values, StringType())
        df = df.withColumn("system_error_message",
                           when(col("mandatory_columns").isNotNull() | (trim(col("mandatory_columns")) != ''),
                                # col("valid") == True,
                                concat(concat_values_udf(col("amount_period_1"), col("amount_period_2"),
                                                         col("amount_period_3"),
                                                         col("amount_period_4"), col("amount_period_5"),
                                                         col("amount_period_6"),
                                                         col("amount_period_7"), col("amount_period_8"),
                                                         col("amount_period_9"),
                                                         col("amount_period_10"), col("amount_period_11"),
                                                         col("amount_period_12"),
                                                         col("mandatory_columns"), col("system_error_message")),
                                       df["system_error_message"])).otherwise(
                               df['system_error_message']))
        self.logger.info('mandatory_if completed')
        df = df.drop("mandatory_columns")
        return df

    def run_step(self, spark, config, context=None, glueContext=None) :
        print("mandatory if handler starting")
        job_name = config.args.get('JOB_NAME')
        df = context.df(self.props.get("target"))
        df2 = context.df(self.props.get("target2"))
        mapping = self.props.get("mapping", "{}")
        columns = self.props.get("columns", "*")
        max_column_limit = self.props.get("max_column_limit", 29)
        validation_rule = self.props.get("validation_rule")
        target = self.props.get("target")
        self.logger.info(f"READING target: {target} ")
        df_bad = None
        if validation_rule == ValidationRule.MANDATORY_IF_CHECK.value :
            df = self.mandatory_if_check(df, df2, self.props.get("mandatory_if_columns").get("columns").split(','))
            # self.props.get("mandatory_if_columns").get("mandatory_if_cols").split(','))
        elif validation_rule == ValidationRule.CHECK_NULL_VALUES.value :
            df = self.check_null_values(df, self.props.get("check_mandatory_columns").get("columns").split(','))
        else :
            raise ValueError('Specify atleast one validation rule')

        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("mandatory if handler ended")

    def register_dataframe(self, context, df, suffix="") :
        if df is not None :
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)


class ValidationRule(Enum) :
    MANDATORY_IF_CHECK = "mandatory_if_check"
    CHECK_NULL_VALUES = "check_null_values"