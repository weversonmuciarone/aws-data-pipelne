from .base_step import Step
from .observability import StepMetric
import json
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as f
from enum import Enum
from datetime import datetime
from pyspark.sql.functions import *

@Step(
    type="cleanse",
    props_schema={
        "type":  "object",
        "properties": {
            "target": {"type": "string"},
            "cleansing_rule": {"type": "string"},
            "columns": {"type": "string"},
            "filter_nulls": {
                "type": "object",
                "properties": {
                    "columns": {"type": "string"}
                },
                "required": ["columns"]
            },
            "typecast_to_int": {
                "type": "object",
                "properties": {
                    "columns": {"type": "string"}
                },
                "required": ["columns"]
            },
            "mapping": {"type": "string"},
            
            "max_column_limit": {"type": "integer"}
        }
    }
)
class Cleansing:
    def add_column(self,json_string,df):
        self.logger.info('add_columns started')   
        obj=json.loads(json_string)
        for k,v in obj.items():
            if v.lower()=="current_timestamp":
                now = datetime.now()
                dt_string = now.strftime("%Y%m%d%H%M%S")
                df=df.withColumn(k,  f.lit(dt_string))
        
            else:
                df=df.withColumn(k,f.lit(v))

        self.logger.info('add_columns completed')

        return df

    def select_columns(self, json_string, df):
        self.logger.info('select_columns started')   
        obj=json.loads(json_string)
        list_of_cols=obj['list_cols']
        selected_column_df=df.select(*[list_of_cols])
        self.logger.info('select_columns completed')

        return selected_column_df

    def check_multiple_rows(self,json_string,df):
        self.logger.info('check_multiple_rows started') 
        obj=json.loads(json_string)
        group_column=obj['group_by']
        print(type(group_column))
        print(group_column)
        
        df_new=df.groupBy(*group_column).count().filter(col('count')>1)
        
        if df_new.count()>0:
            self.logger.error('The Employee hire file has multiple records for one employee_id')
            raise Exception('The Employee hire file has multiple records for one employee_id')
        
        return df


    def rename_columns(self, json_string, df):

        self.logger.info('rename_columns started')

        obj = json.loads(json_string)

        names = df.schema.names
        new_names = []
        renamed_df = df
        for name in names:
            if name in obj:
                renamed_df = renamed_df.withColumnRenamed(name, obj[name])
            else:
                renamed_df = renamed_df.withColumnRenamed(name, name.replace(' ', '_'))
        self.logger.info('renamed columns : '.join(new_names))
        self.logger.info('rename_columns completed')
        return renamed_df

    def drop_duplicates(self, df, keys):
        self.logger.info('drop_duplicates started')
        df = df.dropDuplicates(keys)
        self.logger.info('drop_duplicates completed')
        return df

    def replace_nulls(self, df):
        self.logger.info('replace_nulls started')
        na_functions = df.na
        self.logger.info('replace_nulls completed')
        return na_functions.fill("")

    def truncate_extra_columns(self, df, number):
        self.logger.info('truncate_extra_columns executed')
        return df.select(df.columns[:number])

    def column_typecast_to_int(self, df, columns_to_be_typecasted):
        self.logger.info('column_typecast_to_int started')
        for colName in columns_to_be_typecasted:
            if colName != "":
                df = df.withColumn(colName, df[colName].cast(IntegerType()))
        self.logger.info('column_typecast_to_int completed')
        return df

    def filter_nulls(self, df_input, col_name):
        self.logger.info('filter_nulls started')

        if isinstance(col_name, str):
            df_calcnull = df_input.withColumn('calcnull', (f.trim(f.col(col_name)) == '') | (f.col(col_name).isNull()))
            df_nulls = df_calcnull.filter('calcnull').drop('calcnull')
            df_nonnulls = df_calcnull.filter('not calcnull').drop('calcnull')
            self.logger.info('filter_nulls completed')
            return (df_nonnulls, df_nulls)

        elif isinstance(col_name, list):
            df_nonnulls = df_input
            df_nulls = df_input.limit(0)

            for c in col_name:
                dnn, dn = self.filter_nulls(df_nonnulls, c)
                df_nonnulls = dnn
                df_nulls = df_nulls.union(dn)

            self.logger.info('filter_nulls completed')
            return df_nonnulls, df_nulls
        else:
            self.logger.error('filter_nulls : Either a column name string or a list of column names is expected')
            raise ValueError('Either a column name string or a list of column names is expected')

    def run_step(self, spark,config ,context=None, glueContext=None):
        job_name = config.args.get('JOB_NAME')
        df = context.df(self.props.get("target"))
        mapping = self.props.get("mapping", "{}")
        columns = self.props.get("columns", "*")
        max_column_limit = self.props.get("max_column_limit", 29)
        cleansing_rule = self.props.get("cleansing_rule")
        target = self.props.get("target")
        self.logger.info(f"READING target: {target} ")
        df_bad = None
        if cleansing_rule == CleansingRule.RENAME_COLUMNS.value:
            df = self.rename_columns(mapping, df)
        elif cleansing_rule == CleansingRule.ADD_COLUMN.value:
            df = self.add_column(mapping,df)
        elif cleansing_rule == CleansingRule.CHECK_MULIPLE_ROWS.value:
            df = self.check_multiple_rows(mapping,df)
        elif cleansing_rule == CleansingRule.SELECT_COLUMNS.value:
            df = self.select_columns(mapping, df)
        elif cleansing_rule == CleansingRule.DROP_DUPLICATES.value:
            df = self.drop_duplicates(df, columns.split(','))
        elif cleansing_rule == CleansingRule.TRUNCATE_EXTRA_COLUMNS.value:
            df = self.truncate_extra_columns(df, max_column_limit)
        elif cleansing_rule == CleansingRule.FILTER_NULLS.value:
            df, df_bad = self.filter_nulls(df, self.props.get("filter_nulls").get("columns").split(','))
        elif cleansing_rule == CleansingRule.TYPECAST_TO_INT.value:
            if self.props.get("typecast_to_int", "") != "":
                df = self.column_typecast_to_int(df, self.props.get("typecast_to_int").get("columns", "").split(','))
        elif cleansing_rule == CleansingRule.REPLACE_NULLS.value:
            df = self.replace_nulls(df)
        elif cleansing_rule == CleansingRule.ALL.value:
            df = self.rename_columns(mapping, df)
            df = self.select_columns(mapping, df)
            df = self.drop_duplicates(df, columns.split(','))
            if self.props.get("typecast_to_int","") != "":
                df = self.column_typecast_to_int(df, self.props.get("typecast_to_int").get("columns", "").split(','))
            df = self.truncate_extra_columns(df, max_column_limit)
            df, df_bad = self.filter_nulls(df, self.props.get("filter_nulls").get("columns").split(','))
            df = self.replace_nulls(df)
        else:
            raise ValueError('Specify atleast one cleansing rule')

        self.register_dataframe(context, df)
        self.register_dataframe(context, df_bad, "_bad")
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name+suffix)
            context.register_df(self.name+suffix, df)


class CleansingRule(Enum):
    ADD_COLUMN = "add_column"
    DROP_DUPLICATES = "drop_duplicates"
    RENAME_COLUMNS = "rename_columns"
    TYPECAST_TO_INT = "typecast_to_int"
    TRUNCATE_EXTRA_COLUMNS = "truncate_extra_columns"
    REPLACE_NULLS = "replace_nulls"
    FILTER_NULLS = "filter_nulls"
    SELECT_COLUMNS = "select_columns"
    CHECK_MULIPLE_ROWS = "check_multiple_rows"
    ALL = "all"
