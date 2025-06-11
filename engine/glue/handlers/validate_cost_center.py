from .base_step import Step, StepMetric
from jinja2 import Template

TEMPLATE_VALIDATOR_SQL = 'plandata_finance/validate_cost_center.sql.template'

KEY_DATASET_COST_CENTER = 'dataset_cost_center'

KEY_DATASET_PLAN = 'dataset_plan'

@Step(
    type="plandata_finance.validate_cost_center",
    props_schema={
        "type":  "object",
        "properties":{
            "target": { "type": "string" },
            "dataset_cost_center": { "type": "string" }
        },
        "required":["target","dataset_cost_center"]
    }
)
class CostCenterValidator:

    def run_step(self, spark,config,context=None, glueContext=None):
        job_name = config.args.get('JOB_NAME')
        target = self.props.get('target')

        query = config.get_query(TEMPLATE_VALIDATOR_SQL)
        rendering_vars = {
                KEY_DATASET_COST_CENTER: self.props.get('dataset_cost_center'),
                KEY_DATASET_PLAN: target
                }
        rendering_vars.update(config.variables)
        self.check_sql_injection(rendering_vars)
        query = Template(query).render(rendering_vars)

        df = spark.sql(query)
        # print("cc dataset count")
        # print(KEY_DATASET_PLAN.count())
        print("cc df count")
        print(df.count())
        df.createOrReplaceTempView(self.name)
        context.register_df(self.name, df)
        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count",
                                    value=df.rdd.countApprox(timeout = 800, confidence = 0.5)))

    def check_sql_injection(self, rendering_vars):
        if False:
            self.logger.error('BAD PARAMETER')
            raise Exception('BAD PARAMETER')