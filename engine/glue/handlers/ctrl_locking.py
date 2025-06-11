from pyspark.sql.functions import lit, col, when, concat, trim

from .base_step import Step
from .observability import StepMetric
import json
import datetime
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as f
from enum import Enum

@Step(
    type="check_lock_date",
    props_schema={
        "type":  "object",
        "properties": {
            "target1": {"type": "string"},
            "target2": {"type": "string"},
            "validation_rule": {"type": "string"},
            }
    }
)
class LockDateValidation:

    # perform row level check to ensure that the value is populated for the given field
    # If there is null values, create a column - "system_error_message" or "error_message"
    def check_lock_date(self, error_col, df1, df2):
        self.logger.info('check_lock_date started')
        df3 = df2.select("fiscal_year","plan_version", "plan_type").distinct()
        df1 = df1.select("year", "plan_ver", "plan_typ","lock_dt")
        currentdate = datetime.datetime.today().strftime('%Y-%m-%d')
        lkp_lockdate = df3.join(df1, (df1.plan_ver == df3.plan_version) &
                                         (df1.plan_typ == df3.plan_type) &
                                         (df1.year == df3.fiscal_year))
        bad_records = lkp_lockdate.filter(from_unixtime(unix_timestamp(f.col('lock_dt'), 'yyyy-MM-dd')) < currentdate)
        # bad_records.show()
        error_column = error_col
        if error_column is None:
            raise Exception("Variable error_column is undefined!")
        '''
        Fix for https://issues.apache.org/jira/browse/SPARK-14948
        This error occurred in Glue Job
        dk-mgmt-plan-uploadtest-plan_upload_validation_DK-test
        (run id jr_eaddf97a5223a98d5599be23d64d7510b9a48e1caad21c9419635984f5eec470)
        (commit id dk-mgmt-plan-upload/af9f935579629af3361f69544ed42c138ee0f5b1).
        '''
        df2 = df2.rdd.toDF()
        final_records1 = df2.join(bad_records, (df2.plan_version == bad_records.plan_ver) &
                                  (df2.plan_type == bad_records.plan_typ) &
                                  (df2.fiscal_year == bad_records.fiscal_year),
                                          "left_semi").withColumn(error_column, lit("lock date already passed for this Version and Value Type"))
        final_records2 = df2.drop(error_column).exceptAll(final_records1.drop(error_column)).withColumn(error_column,lit(''))
        final_records = final_records1.union(final_records2)
        # final_records.show()
        self.logger.info('check_lock_date completed')
        return final_records

    def run_step(self, spark,config ,context=None, glueContext=None):
        print("check_lock_date started")
        job_name = config.args.get('JOB_NAME')
        df1 = context.df(self.props.get("target1"))
        df2 = context.df(self.props.get("target2"))
        mapping = self.props.get("mapping", "{}")
        columns = self.props.get("columns", "*")
        max_column_limit = self.props.get("max_column_limit", 29)
        validation_rule = self.props.get("validation_rule")
        target1 = self.props.get("target1")
        self.logger.info(f"READING target1: {target1} ")
        target2 = self.props.get("target2")
        self.logger.info(f"READING target2: {target2} ")
        df_bad = None
        if validation_rule == ValidationRule.CHECK_LOCK_DATE.value:
            df = self.check_lock_date(config.get_variable('error_column'), df1,df2)
        else:
            raise ValueError('Value Type version combination passed lock date')
        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))
        print("check_lock_date ended")

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name+suffix)
            context.register_df(self.name+suffix, df)

class ValidationRule(Enum):
    CHECK_LOCK_DATE = "check_lock_date"