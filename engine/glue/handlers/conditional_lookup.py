from .base_step import Step
from pyspark.sql import functions as f


@Step(
    type="conditional_lookup",
    props_schema={
        "type" : "object",
        "properties" : {
            "target" : {"type" : "string"},
            "field" : {"type" : "string"},
            "gludDb" : {"type" : "string"},
            "glueTable" : {"type" : "string"},
            "glueColumn" : {"type" : "string"},
            "conditionalColumn" : {"type" : "string"},
            "conditionalValue" : {"type" : "string"},
        },
        "required" : ["target", "field", "glueDb", "glueTable", "glueColumn", "conditionalColumn"]
    }
)
class ConditionalLookup :
    def run_step(self, spark, config, context=None, glueContext=None) :
        print("conditional lookup starting")
        job_name = config.args.get('JOB_NAME')
        prefix = f"{self.name} [{self.type}]"

        target = self.props.get("target")
        field = self.props.get("field")
        glueDb = self.props.get("glueDb")
        glueTable = self.props.get("glueTable")
        glueColumn = self.props.get("glueColumn")
        conditional_column = self.props.get("conditionalColumn")
        conditional_value = self.props.get("conditionalValue","P")
        error_column = config.get_variable('error_column')
        if error_column is None :
            raise Exception("Variable error_column is undefined!")

        cols_target = context.df(target).columns
        joined_col_name = 'lookup_res'

        query_join = self.build_query(target, cols_target, field, glueDb, glueTable, glueColumn, joined_col_name)
        df_joined = spark.sql(query_join)
        # df_joined = df_joined.withColumn(
        #         error_column,
        #         f.concat(
        #                 error_column,
        #                 f.when(
        #                         ((~f.isnull(field)) & f.isnull(joined_col_name)) & (f.trim(f.col(field)) != ''),
        #                         f.when(f.length(error_column) == 0, f'{field} is invalid').otherwise(f.lit(f', {field} is invalid'))
        #                         )\
        #                 .otherwise(f.lit(''))
        #                 )
        #         )
        print("Showing df in conditional lookup")
        # df_joined.show()
        df_joined = df_joined.withColumn(
            error_column,
            f.concat(
                error_column,
                f.when(f.col(conditional_column) == conditional_value,
                       f.when(((~f.isnull(field)) & f.isnull(joined_col_name)) & (f.trim(f.col(field)) != ''),
                              f.when(f.length(error_column) == 0, f'{field} is invalid')
                              .otherwise(f.lit(f', {field} is invalid'))
                              ).otherwise(f.lit(''))
                       ).otherwise(f.lit(''))
            )
        )

        df = df_joined.drop(joined_col_name)

        df.createOrReplaceTempView(self.name)
        context.register_df(self.name, df)
        print("conditional lookup ended")

    def build_query(self, target, cols_target, field, glueDb, glueTable, glueColumn, joined_col_name) :
        '''
        Builds and returns a join query.
        The query will compute a left equijoin between target and glueTable
        on 'field'='glueColumn', and project all columns of target along
        with glueColumn from glueTable (or null where the value is not present
        in glueTable').
        '''
        alias_target = 'l'
        alias_glueTable = 'r'
        projections_from_left = [f'{alias_target}.{c}' for c in cols_target]
        projection_from_right = f'{alias_glueTable}.{glueColumn} as {joined_col_name}'

        projection = ', '.join(projections_from_left + [projection_from_right])

        query = f'select {projection}' \
                + f'\nfrom {target} {alias_target}' \
                + f'\nleft join (select distinct if({glueColumn} rlike "^[0-9]+$", regexp_replace({glueColumn}, "^0+", ""), {glueColumn}) as {glueColumn} from {glueDb}.{glueTable}) {alias_glueTable}' \
                + f'\non {alias_target}.{field} = {alias_glueTable}.{glueColumn}'

        print('Created query:')
        print(query)

        return query