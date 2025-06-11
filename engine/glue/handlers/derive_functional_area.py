from pyspark.sql.functions import lit, col, when, concat, trim, substring_index

from .base_step import Step
from .observability import StepMetric
import json
import datetime
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as f
from enum import Enum


@Step(
    type="derive_functional_area",
    props_schema={
        "type": "object",
        "properties": {
            "target1": {"type": "string"},
            "target2": {"type": "string"},
            "target3": {"type": "string"},
            "target4": {"type": "string"},
            "target5": {"type": "string"},
            "validation_rule": {"type": "string"},
        }
    }
)
class FunctionalAreaValidation:

    # perform row level check to ensure that the value is populated for the given field
    # If there is null values, create a column - "system_error_message" or "error_message"
    def derive_functional_area(self, error_col, df1, df2, df3, df4, df5):
        self.logger.info('derive_functional_area started')
        error_column = error_col
        if error_column is None:
            raise Exception("Variable error_column is undefined!")
        df_p = df1.filter(col("account_type") == 'P')
        df_b = df1.filter(col("account_type") == 'B')
        df_n = df1.filter(col("account_type").isNull())
        df9 = df_p.where(f.length(col("functional_area")) == 0) \
            .where(f.length(col("order_type")) == 0).where(f.length(col("pm_activity_type")) == 0)

        df10 = df_p.where(f.length(col("functional_area")) > 0)

        gl_account_fa = df9.drop("functional_area").alias("fst").join(df5, (df9.gl_account == df5.gl_account), "left") \
            .select("fst.*", df5.functional_area)

        df11 = gl_account_fa.where(f.length(col("functional_area")) > 0)
        fa_df = df9.join(df5, df9.gl_account == df5.gl_account, "left_anti")

        final_df = gl_account_fa.unionByName(df10).unionByName(df_b).unionByName(df_n)

        if fa_df.count() > 0:

            df6 = fa_df.where(f.length(col("cost_center")) > 0).where(col("order_no").startswith('7') |
                                                                      col("order_no").startswith('8') |
                                                                      col("order_no").startswith('9') | (
                                                                                  f.length(col("order_no")) == 0)) \
                .where(f.length(col("wbs_element")) == 0)

            df7 = fa_df.where(f.length(col("order_no")) > 0).where(
                col("order_no").startswith('5') | col("order_no").startswith('2'))

            df8 = fa_df.where(f.length(col("wbs_element")) > 0).where(col("wbs_element").startswith('O')) \
                .where(col("wbs_element").endswith('R'))

            df9 = fa_df.where(f.length(col("cost_center")) > 0).where(col("wbs_element").startswith('I') |
                                                                      col("wbs_element").startswith('P') |
                                                                      (col("wbs_element").startswith('O') & col(
                                                                          "wbs_element").endswith('C')) |
                                                                      (col("wbs_element").startswith('O') & col(
                                                                          "wbs_element").endswith('P')))
            df_fa = df6.unionByName(df9)
            df_fa = df_fa.rdd.toDF()
            cost_centre_pc = df_fa.drop("functional_area").alias("fst").join(df2, (df_fa.cost_center == df2.cost_center) &
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
                .select("fst.*", substring_index(df2.functional_area_srkey, 'SAP', -1).alias("functional_area"))
            cost_centre_pc_fa = cost_centre_pc.where(trim(f.length(col("functional_area"))) > 0)

            cost_centre_pc2 = df_fa.drop("functional_area").alias("fst1").join(df2, (df_fa.cost_center == df2.cost_center) &
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
                .select("fst1.*", substring_index(df2.functional_area_srkey, 'SAP', -1).alias("functional_area"))
            cost_centre_pc2_fa = cost_centre_pc2.where(trim(f.length(col("functional_area"))) > 0)

            cost_order_fa = df7.drop("functional_area").alias("fst2").join(df3, (df7.order_no == df3.order_number),
                                                                           "left") \
                .select("fst2.*", substring_index(df3.functional_area_srkey, 'SAP', -1).alias("functional_area"))
            cost_order_farea = cost_order_fa.where(f.length(trim(col("functional_area"))) > 0)

            wbs_element_fa = df8.drop("functional_area").alias("fst3").join(df4,
                                                                            (df8.wbs_element == df4.wbs_element_lkp),
                                                                            "left") \
                .select("fst3.*", substring_index(df4.functional_area_srkey, 'SAP', -1).alias("functional_area"))
            wbs_element_farea = wbs_element_fa.where(f.length(trim(col("functional_area"))) > 0)

            merge_records = cost_centre_pc_fa.unionByName(cost_centre_pc2_fa).unionByName(cost_order_farea).unionByName(wbs_element_farea)

            joining_cols = ["period_from", "period_to", "fiscal_year", "company_code", "gl_account", "plan_version", \
                            "plan_type", "profit_center", "cost_center", "order_no", "order_type", "pm_activity_type", \
                            "wbs_element", "trading_partner", "movement_type", "controller_text", \
                            "currency_key", "amount_period_1", "amount_period_2", "amount_period_3", "amount_period_4", \
                            "amount_period_5", "amount_period_6", "amount_period_7", "amount_period_8",
                            "amount_period_9", "amount_period_10", "amount_period_11", "amount_period_12", "plan_key"]

            validation_df = fa_df.join(merge_records, joining_cols, "left_anti") \
                .withColumn("system_error_message",
                            when(f.length(col('system_error_message')) == 0, lit("functional area cannot be derived"))
                            .otherwise(concat(fa_df.system_error_message, lit(" ,functional area cannot be derived"))))

            functional_df = merge_records.unionByName(validation_df).unionByName(df11).unionByName(df10) \
                .unionByName(df_b).unionByName(df_n)
            self.logger.info('derive_functional_area completed')
            return functional_df
        else:
            return final_df

    def run_step(self, spark, config, context=None, glueContext=None):
        print("derive functional area handler starting")
        job_name = config.args.get('JOB_NAME')
        df1 = context.df(self.props.get("target1"))
        df2 = context.df(self.props.get("target2"))
        df3 = context.df(self.props.get("target3"))
        df4 = context.df(self.props.get("target4"))
        df5 = context.df(self.props.get("target5"))
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
        target5 = self.props.get("target5")
        self.logger.info(f"READING target5: {target5} ")
        df_bad = None
        if validation_rule == ValidationRule.CHECK_FUNCTIONAL_AREA.value:
            df = self.derive_functional_area(config.get_variable('error_column'), df1, df2, df3, df4, df5)
        else:
            raise ValueError('Functional Area cannot be derived')
        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("derive functional area handler ended")

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)

class ValidationRule(Enum):
    CHECK_FUNCTIONAL_AREA = "derive_functional_area"