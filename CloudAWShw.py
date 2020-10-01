import boto3
import csv
import hashlib
import urllib3
import pandas as pd
import fsspec
import os
from io import StringIO
from boto3.dynamodb.conditions import Key
from pprint import pprint

s3 = boto3.resource('s3', aws_access_key_id='AKIAJPZTMGP44IRTRI3A', aws_secret_access_key='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')

try:
    s3.create_bucket(Bucket='datacont-name', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

bucket = s3.Bucket("datacont-name")

bucket.Acl().put(ACL='public-read')

body = open('awsTest.csv', 'rb')
o = s3.Object('datacont-name', 'test').put(Body=body)
s3.Object('datacont-name', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
                       region_name='us-west-2',
                       aws_access_key_id='AKIAJPZTMGP44IRTRI3A',
                       aws_secret_access_key='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'
                       )

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    # if there is an exception, the table may already exist.
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open('csvExperiment.csv', "rt", encoding="utf-8") as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')

    for item in csvf:
        print("item 3 is: " + item[3])

        body = open(item[3], 'rb')
        s3.Object('datacont-name', item[3]).put(Body=body)


        md = s3.Object('datacont-name', item[3]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/datacont-name/" + item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                         'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

['experiment1', '1', '3/15/2002', 'exp1', 'this is the comment']
['experiment1', '2', '3/15/2002', 'exp2', 'this is the comment2']
['experiment2', '3', '3/16/2002', 'exp3', 'this is the comment3']
['experiment3', '4', '3/16/2002', 'exp4', 'this is the comment233']


print("Query 1: ")
response = table.get_item( Key={
'PartitionKey': 'Exp2',
'RowKey': '2' }
)
print(response)
item = response['Item']
print(item)

response2 = table.get_item( Key={
'PartitionKey': 'Exp1',
'RowKey': '1' }
)
print("Query 2: ")
print(response2)
