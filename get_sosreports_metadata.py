import json
import os
from collections import defaultdict

import boto3

from source_buckets import source_buckets


class GenerateSOSMeta:
    def __init__(self, aws_profile='default'):
        os.environ['AWS_PROFILE'] = aws_profile
        self.s3_client = boto3.client(
            's3', endpoint_url='https://s3.upshift.redhat.com')
        self.output_dir = 'output_test_scripts/sosreports_metadata'

    def get_directories(self, s3_object):
        dirnames = os.path.dirname(s3_object)
        dirnames = dirnames.split('/')
        directories = []
        for directory_level in range(len(dirnames)):
            directories.append('/'.join(dirnames[:directory_level+1]))
        return directories

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

    def save_to_file(self, directories, prefix_path):
        file_name = os.path.join(self.output_dir, os.path.dirname(
            prefix_path), os.path.basename(prefix_path))[:-1] + '.json'
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'w') as fp:
            json.dump(directories, fp)
        print('Prefix Path: {}, File Name: {}'.format(prefix_path, file_name))

    def convert_bytes_to_megabytes(self, bytes_size):
        """Convert bytes to Mega Bytes

        Args:
            bytes_size ([int]): Byte size of the S3 object
        """
        return(
            bytes_size/float(1 << 20)
        )

    def form_path_string(self, bucket_name, repartitioned_data=False):
        table_name = bucket_name[3:-4].lower().replace('-', '_')
        print('table_name: {}'.format(table_name))
        if not repartitioned_data:
            path_string = 'extraction/sos/parquet/{}/'.format(table_name)
        else:
            path_string = 'testing_repartitioning/extraction/sos/parquet/{}/'.format(
                table_name)
        return path_string

    def store_object_metadata(self, bucket_name, prefix_path):
        directory_metadata = defaultdict(lambda: defaultdict(int))
        objects = self.list_s3_objects(
            bucket_name=bucket_name, prefix_path=prefix_path)
        for object_ in objects:
            object_key, object_size = object_
            directories = self.get_directories(object_key)
            for directory in directories:
                directory_metadata[directory]['size'] += self.convert_bytes_to_megabytes(
                    object_size)
                directory_metadata[directory]['count'] += 1
        self.save_to_file(directory_metadata, prefix_path)


def driver():
    for bucket_number, bucket_name in enumerate(source_buckets):
        print('bucket_number: {}, bucket_name: {}'.format(
            bucket_number, bucket_name))
        generate_sos_metadata = GenerateSOSMeta(aws_profile='DH-ZIPL-CONF-TMP')
        original_prefix_path = generate_sos_metadata.form_path_string(
            bucket_name)
        final_prefix_path = generate_sos_metadata.form_path_string(
            bucket_name, True)
        generate_sos_metadata.store_object_metadata(
            bucket_name, original_prefix_path)
        generate_sos_metadata.store_object_metadata(
            bucket_name, final_prefix_path)


driver()
