from .base_step import Step
from .observability import StepMetric
import boto3
from datetime import datetime
from pyspark.sql.types import LongType, IntegerType, BooleanType, DoubleType, TimestampType, StringType
from pyspark.sql.functions import countDistinct


@Step(
    type="hudi_materialize",
    props_schema={
        "type": "object",
        "required": [
            "bucket", "prefix", "database", "table", "target", "options"
        ],
        "properties": {
            "target": {"type": "string"},
            "path": {"type": "string"},
            "bucket": {"type": "string"},
            "prefix": {"type": "string"},
            "database": {"type": "string"},
            "table": {"type": "string"},
            "pk": {"type": "string"},
            "combine_key": {"type": "string"},
            "hoodie_write_operation": {"type": "string"},
            "input_drop_dups": {"type": "string"}, 
            "commits_retained": {"type": "string"},                       
            "mode": {"type": "string"},
            "description": {"type": "string"},
            "options": {
                "type": "object",
                "properties": {
                    "format": {"type": "string"},
                    "header": {"type": "boolean"},
                    "delimiter": {"type": "string"},
                    "partitions": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["format"],
            }
        },
        "oneOf": [
            {"required": ["path"]},
            {"required": ["bucket", "prefix"]}
        ]

    }
)
class Hudi_Save:
    
    """Materialize to hudi table: This step generated 2 tables in glue catalog
       One with hudi format schema and other with normal parquet format schema (to support historical queries, with suffix _hist)
    """

    # For loading partition to be viewed by client/athena
    # https://stackoverflow.com/questions/50638868/add-a-partition-on-glue-table-via-api-on-aws
    
    def get_glue_table_info(self, database, table):
        """Fetch glue table information"""
        client = boto3.client('glue')
        self.logger.error("Fetching table info for {}.{}".format(database, table))
        response = None
        try:
            response = client.get_table(
                DatabaseName=database,
                Name=table
            )
        except Exception as error:
            self.logger.error("Exception while fetching table info for {}.{} - {}".format(database, table, error))
        return response

    def build_part_loc(self, part_key, part_location, itr_key, depth, part_key_dict, input_list, input_format, output_format, serde_info):
        """Build partiition absoulte path for loading partition to be viewed by client/athena"""        
        self.logger.error("in build_part_loc")
        for value in part_key_dict[part_key]:
            tmp_depth = depth  
            part_location += "{}={}/".format(part_key, value)
            if tmp_depth > 1:
                if len(itr_key) != 0:
                    next_part_key = itr_key[0]
                    if next_part_key is not None:   
                        self.build_part_loc(next_part_key, part_location, itr_key[1:], tmp_depth - 1, part_key_dict, input_list, input_format, output_format, serde_info)
            else:

                input_dict = {
                                'Values': [
                                    value
                                ],
                                'StorageDescriptor': {
                                    'Location': part_location,
                                    'InputFormat': input_format,
                                    'OutputFormat': output_format,
                                    'SerdeInfo': serde_info
                                }
                            }
                self.logger.error(f"in {part_location}")
                self.logger.error(f"in {input_dict}")
                input_list.append(input_dict.copy())

    def generate_partition_input_list(self, df, table_location, input_format, output_format, serde_info, partition_keys):
        """Generate partiition schema information for loading partition to be viewed by client/athena"""  
        input_list = []  

        part_key_dict = {}
        for fld in df.schema.fields:
            print(fld.name)
        if partition_keys:
            print(f'partitions {df.schema.fields}')
            for fld in df.schema.fields:
                self.logger.error(f"part field name {fld.name}")
                if fld.name in partition_keys:
                    if df.select(countDistinct(fld.name).alias(fld.name)).first()[0] < 1000:
                        tmp = df.select(fld.name).distinct().rdd.flatMap(lambda x: x).collect()
                        part_key_dict[fld.name] = tmp
                        self.logger.error(f"part inside if {tmp}")
                    else:
                        self.logger.error(f"else {fld.name}")

        print(part_key_dict)

        # key_depth = 0
        # for part_key in part_key_dict:
        #     itr_key = [*part_key_dict]
        #     # part_key = itr_key[0]
        #     self.logger.error("input glue dict")
        #     key_depth += 1
        #     part_location = "{}/".format(table_location)
        #     self.build_part_loc(itr_key[0], part_location, itr_key[1:], key_depth, part_key_dict, input_list, input_format, output_format, serde_info)
        
        for part_key in part_key_dict:
            self.logger.error("input glue dict")
            for value in part_key_dict[part_key]:  
                part_location = "{}/{}={}".format(table_location, part_key, value)
                self.logger.error(f"part_location {part_location}")
                input_dict = {
                    'Values': [
                        value
                    ],
                    'StorageDescriptor': {
                        'Location': part_location,
                        'InputFormat': input_format,
                        'OutputFormat': output_format,
                        'SerdeInfo': serde_info
                    }
                }
                input_list.append(input_dict.copy())
        return input_list

    def break_list_into_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    def generate_glue_partitions(self, df, database, table, partitions):
        """Generate glue partitions using list of inpur partitions and their values: Helps in mapping"""
        client = boto3.client('glue')
        self.logger.error("start glue table info")
        response = self.get_glue_table_info(database, table)
        self.logger.error("end glue table info")
        input_format = response['Table']['StorageDescriptor']['InputFormat']
        output_format = response['Table']['StorageDescriptor']['OutputFormat']
        table_location = response['Table']['StorageDescriptor']['Location']
        serde_info = response['Table']['StorageDescriptor']['SerdeInfo']
        # partition_keys = response['Table']['PartitionKeys']
        self.logger.error("start glue generate_partition_input_list ")
        partition_input_list = self.generate_partition_input_list(df, table_location, input_format, output_format, serde_info, partitions)
        self.logger.error("end glue generate_partition_input_list ")

        self.logger.error("start glue batch_create_partition ")
        for each_input in self.break_list_into_chunks(partition_input_list, 100):
            partition_response = client.batch_create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInputList=each_input
            )
        self.logger.error("end glue batch_create_partition ")

    def run_step(self, spark, config=None, context=None, glueContext=None):
        prefix = f"{self.name} [{self.type}]"
        job_name = config.args.get("JOB_NAME")

        self.logger.info(
            f"{prefix} SAVE TO {self.props.get('database')}.{self.props.get('table')}")

        df = context.df(self.props.get("target"))

        ref = context.ref(self)
        ref(self.props.get("target"))
        path = ""
        if self.props.get("bucket"):
            path = f"s3://{self.props.get('bucket')}/{self.props.get('prefix')}"
        elif self.props.get("path"):
            path = self.props.get("path")

        dbname = self.props.get('database')
        
        table_name = self.props.get('table')

        pk = self.props.get('pk')

        combine_key = self.props.get('combine_key')

        partition_path = 'default'

        hoodie_write_operation = self.props.get('hoodie_write_operation', 'insert')

        commits_retained = self.props.get('commits_retained', 10)

        input_drop_dups = self.props.get('input_drop_dups', 'false')

        write_mode = self.props.get("mode", "overwrite")

        partitions = self.props.get("partitions")

        self.hudi_save(df, table_name, path, pk, combine_key, partition_path, write_mode, hoodie_write_operation, partitions, commits_retained, input_drop_dups)

        context.register_df(self.name, df)
        #  Generate Hudi and non hudi table schema for catalog (<table_name>, hist_<table_name>)
        self.glue_create_table(df, dbname, table_name, path, partition_path, hoodie_write_operation, partitions)

        self.logger.error("start glue partitions")
        try:
            # Create partition information for Hudi and non hudi table schema for catalog (<table_name>, hist_<table_name>)
            self.generate_glue_partitions(df, dbname, table_name, partitions)
            self.generate_glue_partitions(df, dbname, f'hist_{table_name}', partitions)
        except Exception as e:
            print(f'this is ex {e}')
        self.logger.error("end glue partitions")
        self.update_table_description(dbname,
                                      self.props.get('table'),
                                      config,
                                      self.props.get("description"))

        self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count",
                                    unit="NbRecord", value=df.rdd.countApprox(timeout=800, confidence=0.5)))

    def update_table_description(self, dbname, table_name, config, description):
        if config and config.args.get("ISGLUERUNTIME") and description:

            glue = boto3.client("glue")
            table = glue.get_table(DatabaseName=dbname, Name=table_name)
            table_input = table["Table"]
            update_info = {}

            other_meta_data = ["LastAccessTime",
                               "LastAnalyzedTime",
                               "Name",
                               "Owner",
                               "Parameters",
                               "PartitionKeys",
                               "Retention",
                               "StorageDescriptor",
                               "TableType",
                               "TargetTable",
                               "ViewExpandedText",
                               "ViewOriginalText"]
            update_info["Description"] = description
            for md in other_meta_data:
                if table_input.get(md):
                    update_info[md] = table_input.get(md)

            glue.update_table(DatabaseName=dbname, TableInput=update_info)
        else:
            self.logger.info(
                f"update table description {dbname}, {table_name}, {description}")

    def hudi_save(self, df, table_name, path, pk, combine_key, partition_path, write_mode, hoodie_write_operation, partitions, commits_retained, input_drop_dups):
        """Hudi materialize hudi and non hudi table using Merge on Read with compaction enabled"""
        # hoodie_write_operation = 'insert'
        # commits_retained = 10
        # input_drop_dups = 'false'
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': pk,
            'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
            'hoodie.datasource.write.insert.drop.duplicates': input_drop_dups,
            'hoodie.cleaner.commits.retained': commits_retained,
            'hoodie.compact.inline.max.delta.commits': 1,
            'hoodie.compact.inline': 'true',
            'hoodie.consistency.check.enabled': 'true',
            'hoodie.datasource.write.partitionpath.field': '',
            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',            
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.write.table.name': table_name,
            'hoodie.datasource.write.operation': hoodie_write_operation,
            'hoodie.datasource.write.precombine.field': combine_key,
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }
        if partitions:
            self.logger.info("Partitions by {}".format(str(partitions)))
            partition_path = ','.join(partitions)
            hudi_options['hoodie.datasource.write.partitionpath.field'] = partition_path
            hudi_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.ComplexKeyGenerator'            

        df.write.format("org.apache.hudi").options(
            **hudi_options).mode(write_mode).save(path)

    def get_athena_data_types(self, input_type):
        compatible_data_types = {StringType: 'string', LongType: 'bigint', IntegerType: 'int',
                                 BooleanType: 'boolean', DoubleType: 'double', TimestampType: 'timestamp'}
        if input_type in compatible_data_types.keys():
            return compatible_data_types.get(input_type)
        else:
            raise Exception(f'data type not handled datatype: {input_type}')

    def glue_create_table(self, df, dbname, table_name, path, partition_path, hoodie_write_operation, partitions):

        non_hudi_table_name = f'hist_{table_name}'

        part_keys = []
        if partitions:
            print('partitions')
            for fld in df.schema.fields:
                if fld.name in partitions:
                    part = {
                            'Name': fld.name,
                            'Type': self.get_athena_data_types(type(fld.dataType))
                        }
                    part_keys.append(part)

        hudi_schema = {
            'Name': table_name,
            'Description': f'{table_name} hudi table',
            'Owner': 'hudi',
            'LastAccessTime': datetime(2015, 1, 1),
            'LastAnalyzedTime': datetime(2015, 1, 1),
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'string',
                        'Type': 'string'
                    },
                ],
                # 'Location': f'{path}/{partition_path}',
                'Location': f'{path}',
                'InputFormat': 'org.apache.hudi.hadoop.HoodieParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'Compressed': False,
                'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                }
            },
            'TableType': 'EXTERNAL_TABLE'
        }

        non_hudi_schema = {
            'Name': non_hudi_table_name,
            'Description': f'{non_hudi_table_name}',
            'Owner': 'hudi',
            'LastAccessTime': datetime(2015, 1, 1),
            'LastAnalyzedTime': datetime(2015, 1, 1),
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'string',
                        'Type': 'string'
                    },
                ],
                # 'Location': f'{path}/{partition_path}',
                'Location': f'{path}/',                
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'Compressed': False,
                'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                }
            },
            'TableType': 'EXTERNAL_TABLE'
        }
        if partitions:
            hudi_schema['PartitionKeys'] = part_keys
            non_hudi_schema['PartitionKeys'] = part_keys            

        client = boto3.client('glue')

        class Hudi_Field:
            def __init__(self, name, data_type):
                self.name = name
                self.data_type = data_type

        hudi_commit_time = Hudi_Field('_hoodie_commit_time', StringType)
        hudi_commit_seqno = Hudi_Field('_hoodie_commit_seqno', StringType)
        hui_record_key = Hudi_Field('_hoodie_record_key', StringType)
        hudi_partition_path = Hudi_Field('_hoodie_partition_path', StringType)
        hudi_file_name = Hudi_Field('_hoodie_file_name', StringType)

        hudi_fields = [hudi_commit_time, hudi_commit_seqno,
                       hui_record_key, hudi_partition_path, hudi_file_name]

        column_list = []

        df_fields = df.schema.fields.copy()
        if partitions:
            for field in df.schema.fields:
                if field.name in partitions:
                    df_fields.remove(field)

        for field in hudi_fields:
            column_dict = {'Name': field.name,
                           'Type': self.get_athena_data_types(field.data_type)}
            print(column_dict)
            column_list.append(column_dict)

        for field in df_fields:
            column_dict = {'Name': field.name, 'Type': self.get_athena_data_types(
                type(field.dataType))}
            print(column_dict)
            column_list.append(column_dict)

        hudi_schema['StorageDescriptor']['Columns'] = column_list
        non_hudi_schema['StorageDescriptor']['Columns'] = column_list
        print(hudi_schema)
        print(non_hudi_schema)

        try:
            response = client.delete_table(
                DatabaseName=dbname,
                Name=table_name
            )
            response = client.delete_table(
                DatabaseName=dbname,
                Name=non_hudi_table_name
            )           

        except client.exceptions.EntityNotFoundException as e:
            print(f"Table doesnt exist, Not deleting : {e}")

        response = client.create_table(
            DatabaseName=dbname,
            TableInput=hudi_schema,
        )

        response = client.create_table(
            DatabaseName=dbname,
            TableInput=non_hudi_schema,
        )