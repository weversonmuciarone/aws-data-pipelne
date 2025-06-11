from .base_step import Step
import boto3
import urllib.parse as up

@Step(
    type="archive",
    props_schema={
        "type":  "object",
        "properties":{
            "target" : {"type": "string"},
            "filepath_column" : {"type": "string"},
            "bucket_archive" : {"type": "string"},
            "prefix_archive" : {"type": "string"}
        },
        "required": ["target", "filepath_column", "bucket_archive", "prefix_archive"]
    }
)
class Archive:
    '''
    The archival handler moves the backing S3 files of a file-based dataframe to an
    arhive bucket. This handler does not register any new dataframe.
    The target dataframe is also rendered unusable as it's files no longer exist in
    their original location. Due to these reasons, it is advisable to use this
    handler as a last step in any job configuration.

    This handler requires the target dataframe to have a column containing the s3 file
    path of the source file for each row in it. Such a column can be generated,
    immediately after creating the dataframe from a DataframeReader, by adding a
    column derived from the udf 'input_file_name' provided in the pyspark.sql.functions
    package.
    '''

    def __init__(self, **config):
        self.s3 = boto3.client('s3')
        super().__init__(**config)

    def run_step(self, spark, config, context=None, glueContext= None):
        print("archival handler starting")
        job_name = config.args.get('JOB_NAME')
        prefix=f"{self.name} [{self.type}]"

        target = self.props.get("target")
        filepath_column = self.props.get("filepath_column")
        bucket_archive = self.props.get("bucket_archive")
        prefix_archive = self.props.get("prefix_archive")
        df = context.df(target)

        filepaths_to_archive = [r[filepath_column] for r in df.select(filepath_column).distinct().collect()]
        file_count = len(filepaths_to_archive)
        self.logger.info(f'Found {file_count} {"file" if file_count == 1 else "files"} backing the dataset {target}.')

        for fl in filepaths_to_archive:
            self.logger.info(fl)
            self.move(fl, bucket_archive, prefix_archive)
        print("archival handler ended")

    def move(self, fl, bucket_archive, prefix_archive):
        '''
        fl is an s3 object url string. Eg: 's3://ingestion/plan_upload/latest.csv'
        '''
        parsed_url = up.urlparse(fl)
        bucket_src = parsed_url.netloc
        path_src = parsed_url.path
        key_src = path_src[1:]
        idx_last_sep = key_src.rfind('/')
        basename_src = key_src if idx_last_sep == -1 else key_src[idx_last_sep + 1:]
        try:
            self.s3.copy({'Bucket': bucket_src, 'Key': key_src}, bucket_archive, f'{prefix_archive}/{basename_src}')
            self.s3.delete_object(Bucket=bucket_src, Key=key_src)
        except Exception as e:
            self.logger.error(e)
            print(e)