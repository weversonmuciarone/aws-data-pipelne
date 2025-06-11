from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as stepfunctions
from aws_cdk import aws_stepfunctions_tasks as tasks


def make_glue_job_task(stack, job, stage, bucket_name):
    create_glue_job(stack, job, stage=stack.stage, bucket_name=stack.bucket_name)
    JOB_FLOW = {  '--JOB_FLOW_ID.$' : '$$.Execution.Id'
                    }
    task = tasks.GlueStartJobRun(
        stack,
        f'Glue Job: {job.get("name")}',
        glue_job_name=f'{stack.pipeline_name}-{job.get("name")}-{stack.stage}',
        integration_pattern=stepfunctions.IntegrationPattern.RUN_JOB,
        result_path=stepfunctions.JsonPath.DISCARD,
        arguments=stepfunctions.TaskInput.from_object(JOB_FLOW)
    )
    return task


def create_glue_job(stack, job, stage, bucket_name):
    jobdir = stack.jobdir
    bookmark_property = job.get("properties", {}).get(
        "enable_bookmark", "job-bookmark-disable"
    )
    connection = job.get("properties", {}).get("connection", "")
    if connection:
        return glue.CfnJob(
            stack,
            f'{stack.pipeline_name}-{job.get("name")}-{stage}',
            name=f'{stack.pipeline_name}-{job.get("name")}-{stage}',
            tags=stack.resource_tags,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{bucket_name}/{stage}/engine/glue/glue_main.py",
            ),
            connections=glue.CfnJob.ConnectionsListProperty(connections=[connection]),
            default_arguments={
                "--extra-py-files": f"s3://{bucket_name}/{stage}/engine/glue/deps.zip",
                "--extra-jars": f"s3://{bucket_name}/{stage}/engine/glue/jars/deequ-2.0.2-spark-3.3.jar,s3://{bucket_name}/{stage}/engine/glue/jars/hudi-spark3-bundle_2.12-0.12.1.jar",
                "--enable-glue-datacatalog": "",
                "--extra-files": f"s3://{stack.bucket_name}/{stage}/customcode/glue/glue_jobs/{job.get('config')}",
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--BUCKET_NAME": f"{bucket_name}",
                "--ISGLUERUNTIME": "True",
                "--CONFIGPATH": f"{job.get('config')}",
                "--STAGE": f"{stage}",
                "--job-bookmark-option": bookmark_property,
                "--enable-auto-scaling": job.get("properties", {}).get("enable_auto_scaling", "true"),
            },
            timeout=job.get("properties", {}).get("timeout", 600),
            glue_version='3.0',
            number_of_workers=job.get("properties", {}).get("workers", 2),
            worker_type=job.get("properties", {}).get("worker_type", "G.1X"),
            role=iam.ArnPrincipal(stack.pipeline_iam_role_arn).arn,
        )
    else:
        return glue.CfnJob(
            stack,
            f'{stack.pipeline_name}-{job.get("name")}-{stage}',
            name=f'{stack.pipeline_name}-{job.get("name")}-{stage}',
            tags=stack.resource_tags,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{bucket_name}/{stage}/engine/glue/glue_main.py",
            ),
            default_arguments={
                "--extra-py-files": f"s3://{bucket_name}/{stage}/engine/glue/deps.zip",
                "--extra-jars": f"s3://{bucket_name}/{stage}/engine/glue/jars/deequ-2.0.2-spark-3.3.jar,s3://{bucket_name}/{stage}/engine/glue/jars/hudi-spark3-bundle_2.12-0.12.1.jar",
                "--enable-glue-datacatalog": "",
                "--extra-files": f"s3://{stack.bucket_name}/{stage}/customcode/glue/glue_jobs/{job.get('config')}",
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--BUCKET_NAME": f"{bucket_name}",
                "--ISGLUERUNTIME": "True",
                "--CONFIGPATH": f"{job.get('config')}",
                "--STAGE": f"{stage}",
                "--job-bookmark-option": bookmark_property,
                "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.kryoserializer.buffer.max=128m",
                "--enable-auto-scaling": job.get("properties", {}).get("enable_auto_scaling", "true"),
                "--step_function_name": f'{stack.pipeline_name}',
                "--JOB_FLOW_ID": "default"
            },
            timeout=job.get("properties", {}).get("timeout", 600),
            glue_version="4.0",
            number_of_workers=job.get("properties", {}).get("workers", 2),
            worker_type=job.get("properties", {}).get("worker_type", "G.1X"),
            role=iam.ArnPrincipal(stack.pipeline_iam_role_arn).arn,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                    max_concurrent_runs=30
                 )
        )
