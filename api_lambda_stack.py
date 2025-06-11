import os

from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_iam as _iam
)

from utils.task_group_reader import TaskGroupReader
from utils.variable_reader import VariableReader


class ApiLambdaPipeline(core.Stack):

    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.pipeline = TaskGroupReader(path="config.yaml")
        self.pipeline_name = os.environ.get("PIPELINE_NAME")
        self.accountid = os.environ.get("AWSACCOUNTID")
        self.deploy_accountid = os.environ.get('BASEACCOUNTID')
        self.stage = os.environ.get("STAGE")
        self.role = os.environ.get("ENVROLEARN")
        variables = VariableReader(path="variables.yaml")
        self.central_logging_bucket = os.environ.get("LOGGING_BUCKET", variables.definition['central_logging_bucket'])
        self.stage_name = self.node.try_get_context("stage_variable")
        self.variables_dict = self.node.try_get_context("variables_dict")
        self.create_stage_instance()

    def create_stage_instance(self):
        # creating a lambda role for api
        for resource in self.pipeline.definition.get('rest_api', []):
            config = resource['config']
            self.add_resources(config.get('resources', []))

    def add_resources(self, resources):
        for resource in resources:
            if 'integrations' in resource:
                for integration in resource['integrations']:
                    print("this is integration->", integration)
                    if integration['type'] == 'lambda_function':
                        print("this is integration name->", integration['name'])
                        for config in integration['config'].items():
                            if 'code' in config:
                                code = config[1]
                            if 'handler' in config:
                                handler = config[1]
                        self.create_lambda_with_permissions(self.pipeline_name + "-" + integration['name'], code,
                                                            handler)

    def create_lambda_with_permissions(self, lambda_name, code, handler):

        # creating a lambda role for lambda to write the api response to s3
        response_processor_role = _iam.Role(scope=self,
                                            id=lambda_name + '-role',
                                            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
                                            role_name=lambda_name + '-role'
                                            )

        # adding only one policy with all the required statements
        response_processor_role.attach_inline_policy(
            _iam.Policy(self, 'Policy' + lambda_name, policy_name=lambda_name + "_role_policy",
                        statements=[
                            _iam.PolicyStatement(
                                resources=[
                                    "arn:aws:logs:eu-west-1:*:log-group:/aws/lambda/" + lambda_name + "*:*"],
                                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
                            ),
                            _iam.PolicyStatement(
                                resources=["*"],
                                actions=["lakeformation:*","s3:*", "logs:*", "kms:*", "athena:*", "glue:*"]
                            )
                        ])
        )

        # creating a lambda to write the api response to s3
        response_processor = _lambda.Function(
            self, lambda_name,
            runtime=_lambda.Runtime.PYTHON_3_9,
            function_name=lambda_name,
            description='Lambda function deployed using AWS CDK Python to write api response to s3',
            code=_lambda.Code.from_asset(code),
            handler=handler,
            role=response_processor_role,
            timeout=core.Duration.seconds(600),
            environment={
                # 'bucket_name': self.variables_dict[self.stage_name]["bucket"],
                'stage_name': self.stage
            }
        )
        principal = _iam.ServicePrincipal("apigateway.amazonaws.com")
        response_processor.add_permission(
            id=lambda_name + "-permission",
            principal=principal,
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:eu-west-1:{self.deploy_accountid}:*"
        )
