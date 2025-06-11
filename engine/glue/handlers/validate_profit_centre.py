from pyspark.sql.functions import lit, col, when, concat, trim, substring_index, udf

from .base_step import Step
from .observability import StepMetric
import json
import datetime
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql import functions as f
from enum import Enum


@Step(
    type="check_profit_centre",
    props_schema={
        "type": "object",
        "properties": {
            "target1": {"type": "string"},
            "target2": {"type": "string"},
            "target3": {"type": "string"},
            "target4": {"type": "string"},
            "validation_rule": {"type": "string"},
        }
    }
)
class ProfitCentreValidation:

    # perform row level check to ensure that the value is populated for the given field
    # If there is null values, create a column - "system_error_message" or "error_message"
    def check_profit_centre(self, error_col, df1, df2, df3, df4):
        self.logger.info('check_profit_centre started')
        df1 = df1.rdd.toDF()

        df_p = df1.filter(col("account_type") == 'P')
        df_b = df1.filter(col("account_type") == 'B')
        df_n = df1.filter(col("account_type").isNull())
        df5 = df_p.where(f.length(col("cost_center")) > 0) \
            .where(col("order_no").startswith('7') | col("order_no").startswith('8') | col("order_no").startswith('9')) \
            .where(col("wbs_element").startswith('I') | col("wbs_element").startswith('P') |
                   (col("wbs_element").startswith('O') & col("wbs_element").endswith('C')) |
                   (col("wbs_element").startswith('O') & col("wbs_element").endswith('P')))

        df6 = df_p.where(f.length(col("cost_center")) > 0).where(f.length(col("order_no")) == 0).where(
            f.length(col("wbs_element")) == 0).where(f.length(col("profit_center")) == 0)

        df7 = df_p.where(f.length(col("order_no")) > 0).where(
            col("order_no").startswith('5') | col("order_no").startswith('2'))

        df8 = df_p.where(f.length(col("wbs_element")) > 0).where(col("wbs_element").startswith('O')) \
            .where(col("wbs_element").endswith('R'))

        error_column = error_col
        if error_column is None:
            raise Exception("Variable error_column is undefined!")

        cost_centre_pc = df5.join(df2, (df5.cost_center == df2.cost_center) &
                                  (df5.profit_center != when(f.regexp_replace(df2.profit_center_srkey,
                                                                              'SAP', "") == "", None)
                                   .otherwise(f.regexp_replace(df2.profit_center_srkey, 'SAP', ""))),
                                  "left_semi") \
            .withColumn(error_column, concat(df5.system_error_message,
                                             lit(" ,profit Centre is not the same as the one enclosed in cost centre")))

        cost_centre_pc2 = df6.drop("profit_center").alias("fst1").join(df2, (df6.cost_center == df2.cost_center) &
                                                                       (((f.substring(f.col("valid_from"), 1,
                                                                                      4).cast("int") < f.col(
                                                                           "fiscal_year").cast(
                                                                           'int')) &
                                                                         (f.col("fiscal_year").cast(
                                                                             'int') < f.substring(
                                                                             f.col("valid_to"),
                                                                             1, 4).cast(
                                                                             "int"))) | ((f.substring(
                                                                           f.col("valid_from"), 1,
                                                                           4).cast("int") == f.col(
                                                                           "fiscal_year").cast(
                                                                           'int')) &
                                                                                         (f.col(
                                                                                             "fiscal_year").cast(
                                                                                             'int') < f.substring(
                                                                                             f.col("valid_to"),
                                                                                             1, 4).cast(
                                                                                             "int")) &
                                                                                         (f.substring(
                                                                                             f.col("valid_from"),
                                                                                             6, 7).cast(
                                                                                             "int") <= f.col(
                                                                                             "period_from").cast(
                                                                                             'int')))
                                                                        | ((f.substring(f.col("valid_from"), 1,
                                                                                        4).cast("int") < f.col(
                                                                                   "fiscal_year").cast(
                                                                                   'int')) &
                                                                           (f.col("fiscal_year").cast(
                                                                               'int') < f.substring(
                                                                               f.col("valid_to"),
                                                                               1, 4).cast(
                                                                               "int"))) | ((f.substring(
                                                                                   f.col("valid_from"), 1,
                                                                                   4).cast("int") < f.col(
                                                                                   "fiscal_year").cast(
                                                                                   'int')) &
                                                                                           (f.col(
                                                                                               "fiscal_year").cast(
                                                                                               'int') == f.substring(
                                                                                               f.col("valid_to"),
                                                                                               1, 4).cast(
                                                                                               "int")) &
                                                                                           (f.substring(
                                                                                               f.col("valid_to"),
                                                                                               6, 7).cast(
                                                                                               "int") >= f.col(
                                                                                               "period_to").cast(
                                                                                               'int')))), "left") \
            .select("fst1.*", substring_index(df2.profit_center_srkey, 'SAP', -1).alias("profit_center"))
        cost_centre_pc2_fa = cost_centre_pc2.where(f.length(trim(col("profit_center"))) > 0)

        cost_centre_pc3 = cost_centre_pc2.where(f.length(col("profit_center")) == 0).withColumn(error_column, when(
            f.length(col('system_error_message')) == 0,
            lit("Profit Centre cannot be derived")).otherwise(
            concat(col('system_error_message'), lit(" ,Profit Centre cannot be derived"))))

        cost_centre_pc4 = df6.drop("profit_center").alias("fst").join(df2, (df6.cost_center == df2.cost_center) &
                                                                      (f.substring(f.col("valid_from"), 1, 4).cast(
                                                                          "int") == f.col("fiscal_year").cast(
                                                                          'int')) &
                                                                      (f.col("fiscal_year").cast(
                                                                          'int') == f.substring(f.col("valid_to"),
                                                                                                1, 4).cast(
                                                                          "int")) &
                                                                      (f.substring(f.col("valid_from"), 6, 7).cast(
                                                                          "int") <= f.col("period_from").cast(
                                                                          'int')) &
                                                                      (f.substring(f.col("valid_to"), 6, 7).cast(
                                                                          "int") >= f.col("period_to").cast(
                                                                          'int')), "left") \
            .select("fst.*", substring_index(df2.profit_center_srkey, 'SAP', -1).alias("profit_center"))
        cost_centre_pc4_fa = cost_centre_pc4.where(f.length(trim(col("profit_center"))) > 0)
        cost_centre_pc5 = cost_centre_pc4.where(f.length(col("profit_center")) == 0).withColumn(error_column, when(
            f.length(col('system_error_message')) == 0,
            lit("Profit Centre cannot be derived")).otherwise(
            concat(col('system_error_message'), lit(" ,Profit Centre cannot be derived"))))

        # cost_centre_pc2_fa.show(5)
        cost_order_pc = df7.join(df3, (df7.order_no == df3.order_number) &
                                 (df7.profit_center != when(
                                     f.regexp_replace(df3.profit_centre_srkey, 'SAP', "") == "", None)
                                  .otherwise(f.regexp_replace(df3.profit_centre_srkey, 'SAP', ""))),
                                 "left_semi") \
            .withColumn(error_column, when(f.length(col('system_error_message')) == 0,
                                           lit("Profit Centre is not the same as the one enclosed in CO order"))
                        .otherwise(
            concat(df7.system_error_message, lit(" ,Profit Centre is not the same as the one enclosed in CO order"))))

        wbs_element_df = df8.join(df4, (df8.wbs_element == df4.wbs_element_lkp) &
                                  (df8.profit_center != when(
                                      f.regexp_replace(df4.profit_centre_srkey, 'SAP', "") == "",
                                      None)
                                   .otherwise(f.regexp_replace(df4.profit_centre_srkey, 'SAP', ""))),
                                  "left_semi") \
            .withColumn(error_column, when(f.length(col('system_error_message')) == 0,
                                           lit("Profit Centre is not the same as the one enclosed in wbs element"))
                        .otherwise(concat(df8.system_error_message,
                                          lit(" ,Profit Centre is not the same as the one enclosed in wbs element"))))

        merge_records = cost_centre_pc.unionByName(cost_centre_pc2_fa).unionByName(cost_centre_pc3) \
            .unionByName(cost_centre_pc4_fa).unionByName(cost_centre_pc5).unionByName(cost_order_pc).unionByName(
            wbs_element_df)

        joining_cols = ["period_from", "period_to", "fiscal_year", "company_code", "gl_account", "plan_version",
                        "plan_type", "cost_center", "order_no", "order_type", "pm_activity_type",
                        "wbs_element", "functional_area", "trading_partner", "movement_type", "controller_text",
                        "currency_key", "amount_period_1", "amount_period_2", "amount_period_3", "amount_period_4",
                        "amount_period_5", "amount_period_6", "amount_period_7", "amount_period_8", "amount_period_9",
                        "amount_period_10", "amount_period_11", "amount_period_12", "plan_key"]
        validation_df = df_p.join(merge_records, joining_cols, "left_anti")

        pl_df = merge_records.unionByName(validation_df)

        final_df = pl_df.union(df_b).union(df_n)

        self.logger.info('check_profit_centre completed')
        return final_df

    def run_step(self, spark, config, context=None, glueContext=None):
        print("profit center validation handler starting")
        job_name = config.args.get('JOB_NAME')
        df1 = context.df(self.props.get("target1"))
        df2 = context.df(self.props.get("target2"))
        df3 = context.df(self.props.get("target3"))
        df4 = context.df(self.props.get("target4"))
        mapping = self.props.get("mapping", "{}")
        columns = self.props.get("columns", "*")
        max_column_limit = self.props.get("max_column_limit", 29)
        validation_rule = self.props.get("validation_rule")
        target1 = self.props.get("target1")
        self.logger.info(f"READING target1: {target1} ")
        target2 = self.props.get("target2")
        self.logger.info(f"READING target2: {target2} ")
        target3 = self.props.get("target3")
        self.logger.info(f"READING target3: {target3} ")
        target4 = self.props.get("target4")
        self.logger.info(f"READING target4: {target4} ")
        if validation_rule == ValidationRule.CHECK_PROFIT_CENTRE.value:
            df = self.check_profit_centre(config.get_variable('error_column'), df1, df2, df3, df4)
        else:
            raise ValueError('Profit centre not same as enclosed in cost object')
        self.register_dataframe(context, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("profit center validation handler ended")

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)


class ValidationRule(Enum):
    CHECK_PROFIT_CENTRE = "check_profit_centre"