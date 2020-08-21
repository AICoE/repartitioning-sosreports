import json
import os
from collections import defaultdict

import boto3

from source_buckets import source_buckets


class ListFailedBuckets:
    """Lists all the buckets that were not re-partitioned due to some errors
    or any other reason.
    """

    def __init__(self, aws_profile='default'):
        os.environ['AWS_PROFILE'] = aws_profile
        self.s3_client = boto3.client(
            's3', endpoint_url='https://s3.upshift.redhat.com')
        self.output_dir = 'output_test_scripts/sosreports_metadata'
        self.object_metadata = defaultdict()

    def check_file_existence(self, bucket_name):
        """Checks whether the _SUCCESS file is present in the repartitioning.

        Args:
            bucket_name (str): name of the bucket
        """
        prefix_path = self.form_path_string(bucket_name)
        response = self.s3_client.list_objects_v2(
            Bucket='DH-SECURE-SOSREPORTS',
            Prefix=prefix_path,
        )
        if response.get('Contents', None):
            for obj in response['Contents']:
                if obj['Key'] == prefix_path:
                    return True
        return False

    def form_table_name(self, bucket_name):
        """forms the table name as per the nomenclature

        Args:
            bucket_name (str): name of the bucket for which corresponding table name is generated.

        Returns:
            str: returns the formed table name.
        """
        table_name = bucket_name[3:-4].lower().replace('-', '_')
        return table_name

    def form_path_string(self, bucket_name):
        """forms the path string for the _SUCCESS file

        Args:
            bucket_name (str): name of the bucket under testing for complete re-partitioning

        Returns:
            str: complete S3 path of the _SUCCESS file to be checked.
        """
        table_name = self.form_table_name(bucket_name)
        path_string = 'extraction/sos/parquet/{}/_SUCCESS'.format(
            table_name)
        return path_string

    def fetch_failed_buckets(self, bucket_list):
        failed_buckets = []
        for bucket_index, bucket_name in enumerate(bucket_list):
            print('Checking {}th bucket: {}'.format(bucket_index, bucket_name))
            if not self.check_file_existence(bucket_name):
                failed_buckets.append(bucket_name)
        if not failed_buckets:
            print('No failed buckets.')
            return
        print('Failed buckets are: ')
        for bucket_name in failed_buckets:
            print(bucket_name)
        return failed_buckets

    def persist_failed_bucket_list(self):
        failed_bucket_list = self.fetch_failed_buckets(source_buckets)
        with open('failed_buckets.txt', 'w') as failed_bucket_file:
            for failed_bucket_name in failed_bucket_list:
                failed_bucket_file.write("%s\n" % failed_bucket_name)


def driver():
    list_failed_buckets = ListFailedBuckets(
        aws_profile='DH-ZIPL-CONF-TMP')
    list_failed_buckets.persist_failed_bucket_list()


driver()
