import json
import os
from collections import defaultdict

import boto3

from source_buckets import source_buckets
from list_failed_buckets import ListFailedBuckets


class GenerateMetadataDifference:
    """calculates the difference b/w object count (for the time being) 
    original and re-partitioned tables.
    """

    def __init__(self, aws_profile='default'):
        os.environ['AWS_PROFILE'] = aws_profile
        self.s3_client = boto3.client(
            's3', endpoint_url='https://s3.upshift.redhat.com')
        self.output_dir = 'output_test_scripts/sosreports_metadata'
        self.object_metadata = defaultdict()

    def list_s3_objects(self, bucket_name, prefix_path):
        """List all the s3 objects in the bucket at a give path.

        Args:
            bucket_name (str): name of the bucket
            prefix_path (str): prefix path from where all the s3 objects are to be listed.

        Returns:
            list: list of all s3 objects in the given prefix path
        """
        paginator = self.s3_client.get_paginator('list_objects')
        pages = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix_path)
        all_objects = []
        for page_index, page in enumerate(pages):
            print('{} page(s) over'.format(page_index))
            try:
                for item in page['Contents']:
                    all_objects.append((item['Key'], item['Size']))
            except KeyError:
                print('Failed to save metadata for bucket: {}, prefix_path: {}'.format(
                    bucket_name, prefix_path))
                all_objects = []
                pass
        return all_objects

    def save_to_file(self):
        """saves all the object count difference for each pertaining bucket in one file.
        """
        for key, value in self.object_metadata.items():
            print('Bucket: {} ====> {}'.format(key, value))
        file_name = os.path.join(
            self.output_dir, 'object_count_difference.json')
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'w') as fp:
            json.dump(self.object_metadata, fp)
        print('File saved at: {}'.format(file_name))
        print('Prefix Path: {}, File Name: {}'.format(prefix_path, file_name))

    def form_table_name(self, bucket_name):
        """forms the table name from the bucket, particular to the SOS report concerned.

        Args:
            bucket_name (str): name of the bucket

        Returns:
            str: extracted table name.

        Eg. 'DH-SYSCTL-TMP' returns 'sysctl'
        """
        table_name = bucket_name[3:-4].lower().replace('-', '_')
        return table_name

    def form_path_string(self, bucket_name, repartitioned_data=False):
        """given a bucket's name, returns the source and destination path
        for the corresponding SOS report.

        Args:
            bucket_name (str): name of the bucket
            repartitioned_data (bool, optional): whether to form the path string for source or for destination. Defaults to False.

        Returns:
            [type]: [description]
        """
        table_name = bucket_name[3:-4].lower().replace('-', '_')
        print('table_name: {}'.format(table_name))
        if not repartitioned_data:
            path_string = 'extraction/sos/parquet/{}/'.format(table_name)
        else:
            path_string = 'repartitioned/extraction/sos/parquet/{}/'.format(
                table_name)
        return path_string

    def get_directories(self, s3_object):
        """given an S3 object, return all the directories.

        Args:
            s3_object (boto3.S3Client): boto3 S3 object

        Returns:
            [list]: list of all the directories involved for the S3 object's path.
        """
        dirnames = os.path.dirname(s3_object)
        dirnames = dirnames.split('/')
        directories = []
        for directory_level in range(len(dirnames)):
            directories.append('/'.join(dirnames[:directory_level+1]))
        return directories

    def store_object_metadata(self, bucket_name):
        """Store object's metadata

        Args:
            bucket_name (str): name of the bucket for which object's metadata is stored.

        Raises:
            ValueError: Raise an error if objects were not fetched.
        """
        # original count
        prefix_path = self.form_path_string(bucket_name)
        original_objects = self.list_s3_objects(
            bucket_name=bucket_name, prefix_path=prefix_path)
        original_count = len(original_objects)
        # repartitioned count
        prefix_path = self.form_path_string(bucket_name, True)
        repartitioned_objects = self.list_s3_objects(
            bucket_name=bucket_name, prefix_path=prefix_path)
        repartitioned_count = len(repartitioned_objects)
        if not original_objects or not repartitioned_objects:
            raise ValueError
        self.object_metadata[bucket_name] = {
            'original_count': original_count,
            'repartitioned_count': repartitioned_count,
            'difference': original_count - repartitioned_count
        }


def driver():
    """Driver function to remove interactive mode and run this large script.
    """
    generate_metadata_difference = GenerateMetadataDifference(
        aws_profile='DH-ZIPL-CONF-TMP')
    list_failed_buckets = ListFailedBuckets('DH-ZIPL-CONF-TMP')
    failed_bucket_list = list_failed_buckets.fetch_failed_buckets(
        source_buckets)
    for bucket_number, bucket_name in enumerate(source_buckets):
        if bucket_name in failed_bucket_list:
            continue
        print('bucket_number: {}, bucket_name: {}'.format(
            bucket_number, bucket_name))
        generate_metadata_difference.store_object_metadata(bucket_name)
    generate_metadata_difference.save_to_file()


driver()
