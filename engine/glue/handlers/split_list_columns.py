from .base_step import Step
from .observability import StepMetric
from pyspark.sql import functions as f
from pyspark.sql.functions import when
from pyspark.sql.functions import lit


@Step(
    type="split_list_columns",
    props_schema={
        "type": "object",
        "properties": {
            "target1": {"type": "string"},
            "col_list_fix": {"type": "string"},
            "col_list_dynamic": {"type": "string"}
        }
    }
)
class SplitListColumns:
    def run_step(self, spark, config, context=None, glueContext=None):

        print("***** Starting Step 1 *****")
        job_name = config.args.get('JOB_NAME')
        df = context.df(self.props.get("target1"))
        col_list_fix = self.props.get("col_list_fix").split(',')
        col_list_dynamic = self.props.get("col_list_dynamic").split(',')

        
        print("******** col_list_fix :", col_list_fix)
        print("******** col_list_dynamic :",col_list_dynamic)

        col_list_dynamic_all = []

        for i in col_list_dynamic :
            col_list_dynamic_all.append(i)
            col_list_dynamic_all.append( i + "_atr_isadded")
            col_list_dynamic_all.append( i + "_atr_isupdated")
            col_list_dynamic_all.append( i + "_atr_isdeleted")


        df_columns = df.columns
        fix_columns = list(set(col_list_fix) - set(df_columns))
        dynamic_columns = list(set(col_list_dynamic_all) - set(df_columns))
        print("******** col_list_fix_not_present_in_df :", fix_columns)
        print("******** col_list_dynamic_not_present_in_df :",dynamic_columns)

        if len(fix_columns)>0:
            print("Inside fix columns condition")
            df_dynamic=df
            for col in fix_columns:
                df_dynamic = df_dynamic.withColumn(col, f.lit(''))
        else:
            df_dynamic=df
        if len(dynamic_columns)>0:
            print("inside dynamic columns condition")
            for col in dynamic_columns:
                df_dynamic = df_dynamic.withColumn(col, f.lit(''))
        


        # df_dynamic.show()

        print("2nd step")

        flag_col_list=[]

        for each in col_list_dynamic:
            flag_col_name=each+"_flag"  
            flag_col_list.append(flag_col_name)
            isadded=each+'_atr_isadded'
            isupdated=each+'_atr_isupdated'
            isdeleted=each+'_atr_isdeleted'

            df_dynamic=df_dynamic.withColumn(flag_col_name,
                                            when(df_dynamic[isadded] == 1,"isadded")
                                            .when(df_dynamic[isupdated] == 1,"isupdated")
                                            .when(df_dynamic[isdeleted] == 1 ,"isdeleted")
                                            .otherwise('None'))


        final_list=col_list_fix+col_list_dynamic+flag_col_list

        final_df=df_dynamic.select(*(final_list))  
        print("******* Final Column List : ", final_df.columns) 

        # df = df_1.unionByName(df_2, allowMissingColumns=True)
        # print("******* Final count is : ", final_df.count())

        self.register_dataframe(context, final_df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df.rdd.countApprox(timeout=800, confidence=0.5)))

    def register_dataframe(self, context, df, suffix=""):
        if df is not None:
            df.createOrReplaceTempView(self.name + suffix)
            context.register_df(self.name + suffix, df)