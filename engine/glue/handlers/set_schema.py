from .base_step import Step
from .observability import StepMetric
import re

@Step(
    type="set_schema",
    props_schema={
        "type":  "object",
        "properties":{
            "schema_ddl" : {"type": "string"},
            "target" : {"type": "string"}
        },
        "required":["schema_ddl","target"]
    }
)
class SetSchema:

    def run_step(self, spark,config,context=None, glueContext=None):
        df = context.df(self.props.get("target"))
        job_name = config.args.get('JOB_NAME')
        ref = context.ref(self)
        ref(self.props.get("target"))
        schema_ddl = self.props.get("schema_ddl")

        schema_dict = {
                'type': 'struct',
                'fields': [ddl_to_dict(colinfo) for colinfo in schema_ddl.split(',')]
                }

        cols = df.columns
        schema_query_components = [f'cast({cols[i]} as {schema_dict["fields"][i]["type"]}) as {schema_dict["fields"][i]["name"]}' for i in range(len(schema_dict['fields']))]
        schema_query = 'select ' + ', '.join(schema_query_components) + ' from ' + self.props.get('target')
        df = spark.sql(schema_query)
        df.createOrReplaceTempView(self.name)
        context.register_df(self.name, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count",unit="NbRecord",value=df.rdd.countApprox(timeout = 800, confidence = 0.5)))


def ddl_to_dict(colinfo):
    '''
    This is a small utility function for converting schema from DDL representation to
    JSON. It expects a column info string in the form of
        colname coltype
    and returns a dict of the form
        { 'name': colname, 'type': coltype, 'nullable': 'true', 'metadata': None }
    '''
    colname, coltype = re.split('\s+', colinfo.strip())
    return {
        'name': colname,
        'type': coltype.lower(),
        'nullable': True,
        'metadata': None
        }