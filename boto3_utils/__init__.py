"""
Utilities to interact with AWS resources.
"""
__title__ = 'boto3_utils'
__version__ = '1.0.0'

from .dynamo import scan_with_back_off_and_jitter as scan_occ
from .dynamo import query_with_back_off_and_jitter as query_occ
from .dynamo import BatchWriterWithBackOffAndJitter as BatchWriter
