import os

from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_iam as _iam,
    aws_apigateway as apigw,
    aws_dynamodb as dynamodb
)
from aws_cdk.aws_apigateway import IntegrationOptions
from utils.task_group_reader import TaskGroupReader
from utils.variable_reader import VariableReader


class ApiPipeline(core.Stack):

    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.pipeline = TaskGroupReader(path="config.yaml")
        self.pipeline_name = os.environ.get("PIPELINE_NAME")
        self.accountid = os.environ.get("AWSACCOUNTID")
        self.stage = os.environ.get("STAGE")
        self.role = os.environ.get("ENVROLEARN")
        variables = VariableReader(path="variables.yaml")
        self.central_logging_bucket = os.environ.get("LOGGING_BUCKET", variables.definition['central_logging_bucket'])
        self.stage_name = self.node.try_get_context("stage_variable")
        self.variables_dict = self.node.try_get_context("variables_dict")
        self.create_stage_instance()

    def create_stage_instance(self):
        for resource in self.pipeline.definition.get('rest_api', []):
            config = resource['config']
            base_api = apigw.RestApi(self, resource['name'] + self.stage, deploy_options={"stage_name": self.stage})
            self.add_resources(base_api,config.get('resources', []),base_api.root)

    def add_resources(self, base_api, resources, parent):
        for resource in resources:
            methods = ['OPTIONS']
            api_resource = parent.add_resource(resource['path'])
            if 'integrations' in resource:
                for integration in resource['integrations']:
                    config = integration['config']
                    param = config.get('param', None)
                    if param:
                        request_parameters = {"integration.request.path."+param : "method.request.path."+param}
                    else:
                        request_parameters = {}
                    if integration['type'] == 'lambda_function':
                        api_resource_integration = apigw.LambdaIntegration(
                            _lambda.Function.from_function_arn(
                                    self, integration['name'],
                                    "arn:aws:lambda:eu-west-1:" + self.accountid + ":function:"+ self.pipeline_name +"-"+integration['name']
                                ),
                            proxy=resource.get('proxy', False),
                            integration_responses=[
                                {
                                    'statusCode': '200',
                                    'responseParameters': {
                                        'method.response.header.Access-Control-Allow-Origin': "'*'",
                                    },
                                }
                            ],
                        )
                    elif integration['type'] == 'http':
                        print("this is integration name->", integration['name'])
                        api_resource_integration = apigw.HttpIntegration(
                            config.get('url'),
                            proxy=resource.get('proxy', False),
                            # options=IntegrationOptions(request_parameters =request_parameters)
                            options=IntegrationOptions(integration_responses=[
                                {
                                    'statusCode': '200',
                                    'responseParameters': {
                                        'method.response.header.Access-Control-Allow-Origin': "'*'",
                                    },
                                }
                            ])
                        )
                    else:
                        print("no integration name found->", integration['name'])
                        return

                    api_resource.add_method(
                        integration['method'],
                        api_resource_integration,
                        method_responses=[
                            {
                                'statusCode': '200',
                                'responseParameters': {
                                    'method.response.header.Access-Control-Allow-Origin': True,
                                },
                            }
                        ]
                    )
                    methods.append(integration['method'])
            self.add_options(methods, api_resource)
            #  child resources in apigw
            if 'resources' in resource:
                self.add_resources(base_api, resource['resources'], api_resource)

    def add_options(cls, methods, api_resource):
        api_resource.add_method(
            'OPTIONS',
            apigw.MockIntegration(
                integration_responses=[
                    {
                        'statusCode': '200',
                        'responseParameters': {
                            'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            'method.response.header.Access-Control-Allow-Origin': "'*'",
                            'method.response.header.Access-Control-Allow-Methods': f"'{(',').join(methods)}'",
                        },
                    }
                ],
                passthrough_behavior=apigw.PassthroughBehavior.WHEN_NO_MATCH,
                request_templates={'application/json': '{"statusCode":200}'},
            ),
            method_responses=[
                {
                    'statusCode': '200',
                    'responseParameters': {
                        'method.response.header.Access-Control-Allow-Headers': True,
                        'method.response.header.Access-Control-Allow-Methods': True,
                        'method.response.header.Access-Control-Allow-Origin': True,
                    },
                }
            ],
        )