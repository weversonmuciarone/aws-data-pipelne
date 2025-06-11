from aws_cdk import aws_lambda_python as lambda_python
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import aws_iam as iam
from aws_cdk.aws_lambda import Code
from aws_cdk import aws_lambda as lambda_
from engine.lambdafx.lambda_mapper import LambdaFxPropsMapper
from engine import resource_task
import textwrap
from aws_cdk import core
from aws_cdk import aws_iam
import uuid
from aws_cdk import aws_stepfunctions as stepfunctions

""" Code where the lambdafx functions are created. It uses LambdaFxPropsMapper to build the parameters of Lambda functions
    and its invocations.
"""


def get_function_logs_name(stack, job):
    """Creates a function name from the job name and the pipeline name.
    Truncates the pipeline name when necessary.
    Parameters
        stack: the containing stack
        job: the job configuration
    """
    if len(job['name']) > 38:
        raise Exception(
            'Name of the function cannot exceed 38 characters {}'.format(job['name'])
        )

    if len(job['name']) + len(stack.pipeline_name) >= 62:
        return f"{stack.pipeline_name[:20]}-{stack.pipeline_name[-4:]}-{job['name']}"
    else:
        return f"{stack.pipeline_name}-{job['name']}"


def default_lambda_logs_description(job, stack):
    """Returns the default lambdafx description given the job name and pipeline name.
    Parameters
        job: the job definition
        stack: the stack englobing the lambdafx
    """
    return 'Python lambdafx created by data.all {} {}'.format(
        job['name'], stack.pipeline_name
    )


def make_lambda_function_logs_task(stack, job):
    """Makes python function and its corresponding invocation.

    Parameters
        stack: the target CDK stack
        job: the job configuration
    """

    # Lambda function for logging
    lambdafx = lambda_.Function(
        stack,
        job['name'],
        code=Code.from_inline(
            textwrap.dedent(
                """
                    import boto3
                    from datetime import datetime
                    import os
                    import json

                    def handler(event, context):
                        print(f"event ====> {event}")
                        # Fetch values
                        timestamp = str(datetime.now())
                        guid = event["execution_id"]
                        job=os.environ.get("JOB_NAME")
                        pipeline=os.environ.get("PIPELINE_NAME")
                        step=os.environ.get("LOG_TYPE")
                        status=os.environ.get("STATUS")
                        if event["Payload"].get("sf-error-info",None):
                            msg = event["Payload"].get("sf-error-info").get("Cause")
                        else:
                            msg=""
                        eventType="Pipeline-Job-Step"

                        # Build JSON object
                        log_info = {
                            "timestamp": timestamp,
                            "guid": guid,
                            "process": pipeline,
                            "job": job,
                            "step": step,
                            "eventType": eventType,
                            "status": status,
                            "msg": msg
                        }

                        # Write to S3
                        central_logging_bucket = os.environ.get("CENTRAL_LOGS_BUCKET")
                        file_name = timestamp + "_" + pipeline + ".json"
                        path = f"central_logs/logs/{file_name}"
                        client = boto3.client('s3')
                        client.put_object(Body=str(json.dumps(log_info)), Bucket=central_logging_bucket, Key=path)
                        
                        if event["Payload"].get("sf-error-info",None):
                            raise Exception("Check Step for Error " + step)
                """
            )
        ),
        handler='index.handler',
        runtime=lambda_.Runtime.PYTHON_3_8,
        memory_size=3008,
        timeout=core.Duration.seconds(60),
        environment=job.get('config').get('environment', []),
        role=aws_iam.Role.from_role_arn(stack, f"{job['name']}Role-{str(uuid.uuid4())[:8]}",
                                        stack.pipeline_iam_role_arn)
    )

    # pipeline_iam_role_arn could access to the lambda
    principal = iam.ArnPrincipal(stack.pipeline_iam_role_arn)

    lambdafx.add_permission(
        f"LambdaPermissionBasic{job.get('name')}",
        principal=principal,
        action='lambda:*',
    )
    # pipeline_fulldev_iam_role could access to the lambda
    principal = iam.ArnPrincipal(stack.pipeline_fulldev_iam_role)

    lambdafx.add_permission(
        f"LambdaPermissionFullDev{job.get('name')}",
        principal=principal,
        action='lambda:*',
    )
    # pipeline_admin_iam_role could access to the lambda
    principal = iam.ArnPrincipal(stack.pipeline_admin_iam_role)

    lambdafx.add_permission(
        f"LambdaPermissionAdmin{job.get('name')}",
        principal=principal,
        action='lambda:*',
    )
    # Tag the lambdafx functions
    stack.set_resource_tags(lambdafx)

    # Make the invocation
    task = tasks.LambdaInvoke(
        stack,
        f"Lambda: {job.get('name')}",
        lambda_function=lambdafx,
        payload=stepfunctions.TaskInput.from_object({
            "Payload": stepfunctions.JsonPath.string_at("$"),
            "execution_id": stepfunctions.TaskInput.from_data_at(
                "$$.Execution.Name"
            ).value,
        }),
        result_path="$.lambdalogs"
    )
    return task
