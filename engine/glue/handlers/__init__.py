from enum import Enum

from .assertion import Assertion
from .base_step import StepHandler, StepInterface
from .dq import Verification
from .file_input import FileInput
from .materialize import Save
from .mock import MockStep
from .obfuscate import Obfuscate
from .observability import StepMetric
from .profiling import Profiler
from .query import Query
from .rename_whitespace_cols import ReplaceWhiteSpaceCols
from .s3_file_input import S3FileInput
from .udf import UserDefinedFunction
from .archival import Archive
from .athena_query import Query
from .base_central_logging import BaseCentralLogging
from .central_logging import CentralLogging
from .cleaner import Cleaner
from .cleanse import Cleansing
from .conditional_lookup import ConditionalLookup
from .ctrl_locking import LockDateValidation
from .derive_functional_area import FunctionalAreaValidation
from .duplicate_record_counter import DuplicateRecordCounter
from .lookup import Lookup
from .mandatory_if_validation import MandatoryIfValidations
from .mandatory_validations import MandatoryValidations
from .materialize_hudi import Hudi_Save
from .materializeAsCSV import Save
from .s3_file_validator import FileValidator
from .schema_validation import SchemaValidation
from .set_schema import SetSchema
from .spice_refresh import SpiceRefresh
from .validate import Validation
from .validate_cost_center import CostCenterValidator
from .validate_profit_centre import ProfitCentreValidation
from .data_anonymize import DataAnonymization
from .archive_conformed_data import ArchiveConformedData
from .archive_s3_bucket import ArchiveS3Bucket
from .reconc_notification import ReconciliationNotification
from .reconc_comparison import ReconciliationComparison
from .pipeline_queue_executor import PipelineQueueExecutor
from .move import move
from .generic_json_parser import GenericJsonParser
from .split_list_columns import SplitListColumns
from .materialize_hive import MaterializeHive
from .trigger_file import TriggerFile

# from .convert_to_parquet import ConvertToParquet
# from .structured_logging import StructuredLogger

StepTypes = Enum(
    'StepTypes', {k.upper(): k for k in StepHandler.get_instance().handlers.keys()}
)
