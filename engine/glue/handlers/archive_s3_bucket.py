import zipfile
from datetime import datetime, date
from io import BytesIO
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzlocal

from .base_step import Step
import boto3


@Step(
    type="archive_s3_bucket",
    props_schema={
        "type": "object",
        "properties": {
            "bucket": {"type": "string"},
            "prefix": {"type": "string"},
            "months_to_retain_logs": {"type": "integer"},
            "archive_location": {"type": "string"},
            "file_extension": {"type": "string"},
            "months_to_purge_archived_logs": {"type": "integer"},
            "archive_sub_folders": {"type": "boolean"}
        },
        "required": ["bucket", "prefix", "months_to_retain_logs", "archive_location", "archive_sub_folders"]
    }
)
class ArchiveS3Bucket:
    # creating zip to archive file
    def create_zip_file(self, bucket_name, bucket_files_prefix, year, month, day, archive_location, archive_sub_folders,
                        file_ext):
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)

        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')  # Getting the files by using paginator
        s3object_list = []
        filter_by = bucket_files_prefix  # Filering the data by using Bucket filter

        iterator = paginator.paginate(Bucket=bucket_name, Prefix=filter_by)
        archive = BytesIO()
        # Iterating through files and zipping it according to the date
        with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zip_archive:
            for page in iterator:
                for obj in page['Contents']:
                    flag = False
                    # checking the correct file to archive with extension and for subfolder
                    if file_ext == "":
                        if not archive_sub_folders:
                            check = obj['Key']
                            if '/' in check[len(bucket_files_prefix):]:
                                flag = False
                            else:
                                flag = True
                        else:
                            flag = True
                    else:
                        if obj['Key'].endswith(file_ext):
                            flag = True
                    # checking the correct file to archive on the basis of their correct date and extension
                    if flag and obj[
                        'LastModified'] < datetime(year, month, day, 0, 0, 0,
                                                   tzinfo=tzlocal()):
                        with zip_archive.open(obj['Key'], 'w') as file1:
                            data = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                            file1.write(data['Body'].read())

                        obj = {'Key': obj['Key']}
                        s3object_list.append(obj)
        archive.seek(0)
        # Storing the zipped file into the desired file
        s3_resource.Object(bucket_name, archive_location + str(datetime.now()) + '.zip').upload_fileobj(
            archive)
        archive.close()

        for x in range(0, len(s3object_list), 1000):
            bucket.delete_objects(Delete={'Objects': s3object_list[x:x + 1000]})

    # Applying lifecycle rule on the archived files
    def apply_life_cycle_rule(self, bucket_name, prefix, days_after_move_to_glacier, months_to_purge_archived_logs):

        client = boto3.client('s3')
        rules = []
        if months_to_purge_archived_logs < 1:
            rule = {
                'Prefix': prefix,
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': days_after_move_to_glacier,
                        'StorageClass': 'DEEP_ARCHIVE'
                    },
                ],
            }
        else:
            rule = {'Expiration': {
                'Days': months_to_purge_archived_logs * 30
            },
                'Prefix': prefix,
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': days_after_move_to_glacier,
                        'StorageClass': 'DEEP_ARCHIVE'
                    },
                ],
            }
        try:

            resp = client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name,
            )

            rules = [resp for resp in resp['Rules'] if resp['Prefix'] != prefix]
            rules.append(rule)
        except:
            rules.append(rule)

        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                'Rules': rules
            },
        )

    def run_step(self, spark, config=None, context=None, glueContext=None):
        job_name = config.args.get('JOB_NAME')
        bucket_name = self.props.get('bucket')
        bucket_files_prefix = self.props.get('prefix')
        months_to_retain_logs = self.props.get('months_to_retain_logs')
        file_ext = self.props.get('file_extension', "")
        bucket_archive_location = self.props.get('archive_location')
        months_to_purge_archived_logs = self.props.get("months_to_purge_archived_logs")
        archive_sub_folders = self.props.get("archive_sub_folders")

        relative_date = date.today() + relativedelta(months=-months_to_retain_logs)

        relative_month = relative_date.month
        relative_year = relative_date.year
        relative_day = relative_date.day
        prefix = str(relative_year) + '-' + str(relative_month)
        archive_location = bucket_archive_location + 'archive' + '/' + prefix + '/'
        days_after_move_to_glacier = 0

        self.create_zip_file(bucket_name,
                             bucket_files_prefix,
                             relative_year,
                             relative_month,
                             relative_day,
                             archive_location,
                             archive_sub_folders,
                             file_ext
                             )
        self.apply_life_cycle_rule(bucket_name, archive_location, days_after_move_to_glacier,
                                   months_to_purge_archived_logs)
