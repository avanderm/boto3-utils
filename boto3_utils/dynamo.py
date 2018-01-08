"""
Module for querying and writing to DynamoDB.
"""
import functools
import random
import time

from boto3.dynamodb.table import BatchWriter
from botocore.exceptions import ClientError

def back_off_and_jitter(function):
    """
    Decorator to retry a boto3 request when a Client error is thrown.
    """
    @functools.wraps(function)
    def _back_off_and_jitter(*args, **kwargs):
        back_off = 1    # initial back off starts at 1 seconds
        cap = 60        # maximum back off is 60 seconds
        base = 1
        nr_of_retries = 10

        attempt = 0
        while True:
            try:
                result = function(*args, **kwargs)
                return result
            except ClientError as error:
                error_code = error.response.get('Error', {}).get('Code')

                if error_code == 'ProvisionedThroughputExceededException':
                    if attempt > nr_of_retries:
                        raise error

                    time.sleep(back_off)
                    back_off = random.randint(0, min(cap, base * 2 ** attempt))
                    attempt += 1
                else:
                    # unrelated to DynamoDB's provisioned throughput
                    raise error

    return _back_off_and_jitter


class BatchWriterWithBackOffAndJitter(BatchWriter):
    """
    Implements optimistic concurrency control (OCC) as specified in aws_. This class provides a
    batch writer that implements exponential back off and jitter (full) to decluster calls made
    to Dynamo.

    .. _aws: https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    def __init__(self, table, flush_amount=25, overwrite_by_pkeys=None):
        super(BatchWriterWithBackOffAndJitter, self).__init__(
            table.table_name,
            table.meta.client,
            flush_amount=flush_amount,
            overwrite_by_pkeys=overwrite_by_pkeys
        )


    def _flush(self):
        if int(len(self._items_buffer) / self._flush_amount) > 0:
            repetitions = int(len(self._items_buffer) / self._flush_amount)

            # flush increments equal to flush amount
            for _ in range(repetitions):
                self._flush_with_back_off_and_jitter()
        else:
            # flush last records
            while self._items_buffer:
                self._flush_with_back_off_and_jitter()


    @back_off_and_jitter
    def _flush_with_back_off_and_jitter(self):
        while self._items_buffer:
            super(BatchWriterWithBackOffAndJitter, self)._flush()


@back_off_and_jitter
def __scan_table(table, **kwargs):
    return table.scan(**kwargs)


def scan_with_back_off_and_jitter(table, **kwargs):
    """
    Performs a DynamoDB table scan using pagination and a combination of back off and jitter to
    deal with too many DynamoDB requests. See aws_ for more details.

    .. _aws: https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    item_counter = 0
    limit = kwargs.get('Limit', -1)     # we still require manual checking due to pagination

    while True:
        try:
            response = __scan_table(table, **kwargs)
        except ClientError as error:
            # not provisioned throughput exceeded
            raise error

        for item in response['Items']:
            yield item

            if limit > 0:
                item_counter += 1
                if limit and item_counter >= limit:
                    raise StopIteration

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
        else:
            kwargs.update(ExclusiveStartKey=last_evaluated_key)


@back_off_and_jitter
def __query_table(table, **kwargs):
    return table.query(**kwargs)


def query_with_back_off_and_jitter(table, **kwargs):
    """
    Performs a DynamoDB table query using pagination and a combination of back off and jitter to
    deal with too many DynamoDB requests. See aws_ for more details.

    .. _aws: https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    item_counter = 0
    limit = kwargs.get('Limit', -1)     # we still require manual checking due to pagination

    while True:
        try:
            response = __query_table(table, **kwargs)
        except ClientError as error:
            # not provisioned throughput exceeded
            raise error

        for item in response['Items']:
            yield item
            item_counter += 1
            if limit and item_counter >= limit:
                raise StopIteration

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
        else:
            kwargs.update(ExclusiveStartKey=last_evaluated_key)
