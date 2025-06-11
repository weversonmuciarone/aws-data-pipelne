import boto3
from .base_step import Step


@Step( 
    type="move", 
    props_schema={ 
        "type": "object", 
        "properties":{
            "from_bucket" : {"type": "string"},
            "to_bucket" : {"type": "string"},
            "from_folder" : {"type": "string"},
            "to_folder" : {"type": "string"}
        } 
    } 
)


class move:
    def run_step(self, spark,config, context=None,  glueContext=None):
        
        # read props
        job_name = config.args.get('JOB_NAME')
        from_bucket = self.props.get("from_bucket")
        to_bucket = self.props.get("to_bucket")
        from_folder = self.props.get("from_folder")
        to_folder = self.props.get("to_folder")
        
        s3 = boto3.resource('s3')
        source_bucket = s3.Bucket(from_bucket)
    
        for obj in source_bucket.objects.filter(Prefix='{0}/'.format(from_folder)):
            print('{0}:{1}'.format(source_bucket.name, obj.key))
            if obj.key != from_folder + '/':
                
                copy_source = {'Bucket': from_bucket,'Key': obj.key}
                s3.Object(to_bucket,  to_folder + '/' + obj.key.split('/')[-1]).copy(copy_source)
                s3.Object(from_bucket, obj.key).delete()