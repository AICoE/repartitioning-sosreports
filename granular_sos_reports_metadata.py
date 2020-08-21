import os
from collections import defaultdict

import boto3
import json


class Granular_SOS_Reports_Metadata:
    def __init__(self, file_name):
        self.file_name = file_name
        self.year_directory = defaultdict(lambda: defaultdict(int))
        self.month_directory = defaultdict(lambda: defaultdict(int))
        self.day_directory = defaultdict(lambda: defaultdict(int))

    def save_to_file(self, granularity, directory):
        file_name = '{}_{}'.format(self.file_name, granularity)
        with open('{}.json'.format(file_name), 'w') as fp:
            json.dump(directory, fp)
        print('File: {} saved!'.format(file_name))

    def add_to_year(self, key, value):
        self.year_directory[key] = {
            'size': value['size'],
            'count': value['count']
        }

    def add_to_month(self, key, value):
        self.month_directory[key] = {
            'size': value['size'],
            'count': value['count']
        }

    def add_to_day(self, key, value):
        self.day_directory[key] = {
            'size': value['size'],
            'count': value['count']
        }

    def read_json(self):
        with open(self.file_name, 'r') as json_file:
            json_data = json.load(json_file)
        for key in json_data.keys():
            if 'created_day' in key:
                self.add_to_day(key, json_data[key])
            elif 'created_month' in key:
                self.add_to_month(key, json_data[key])
            elif 'created_year' in key:
                self.add_to_year(key, json_data[key])
        self.save_to_file('day', self.day_directory)
        self.save_to_file('month', self.month_directory)
        self.save_to_file('year', self.year_directory)
        print('Finished saving all the granularities.')


metadata_json_file = input('Please enter the json file: \n')
granular_SOS_Reports_Metadata = Granular_SOS_Reports_Metadata(
    metadata_json_file)
granular_SOS_Reports_Metadata.read_json()
