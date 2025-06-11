"""
Main application for cdk deploy.
It creates one or more stacks.
"""

import os
from aws_cdk import core

from stack import DataPipeline
from utils.task_group_reader import TaskGroupReader
import re
import json


app = core.App()

# read configuration file
pipeline = TaskGroupReader(path='config.yaml')
group_len = len(pipeline.definition.get('groups', []))
resources_len = len(pipeline.definition.get('aws_resources', []))

# create the pipeline
pipeline_name = os.environ.get('PIPELINE_NAME', 'SampleDataProcessingPipeline')
default_description = f"""'{pipeline_name}/{pipeline.definition.get("name", "")}' pipeline,
                                                {group_len} groups and {resources_len} resources"""
stack = DataPipeline(
    app,
    pipeline_name,
    pipeline,
    description=pipeline.definition.get('description', default_description),
)
if os.path.exists('configs'):
    for fname in os.listdir('configs'):
        if os.path.isfile(fname):
            pipeline = TaskGroupReader(path=f'configs/{fname}')
            group_len = len(pipeline.definition.get('groups', []))
            resources_len = len(pipeline.definition.get('aws_resources', []))

            # create the pipeline
            name = pipeline.definition.get('name', '')
            pipeline_name = (
                    os.environ.get('PIPELINE_NAME', 'SampleDataProcessingPipeline')
                    + '-'
                    + name
            )
            pipeline_name = re.sub('[^A-Za-z0-9-]', '-', pipeline_name)
            default_description = f"""'{pipeline_name}/{pipeline.definition.get("name", "")}' pipeline ,
                                                    {group_len} groups and {resources_len} resources"""
            stack = DataPipeline(
                app,
                pipeline_name,
                pipeline,
                description=pipeline.definition.get('description', default_description),
            )

# Step to add tags by reading the tags.json
try:
    key_value_tags_dict = json.load(open("tags.json"))
    if 'test' == os.environ.get("STAGE"):
        for tag_key, tag_value in key_value_tags_dict['test'].items():
            core.Tags.of(app).add(tag_key, tag_value)
    if 'prod' == os.environ.get("STAGE"):
        for tag_key, tag_value in key_value_tags_dict['prod'].items():
            core.Tags.of(app).add(tag_key, tag_value)
except FileNotFoundError:
    print('tags.json file not found')
except Exception as e:
    print(f'Exception occurred while adding key value tags :: {e}')

app.synth()
