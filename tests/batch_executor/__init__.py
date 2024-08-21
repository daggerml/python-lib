#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from time import sleep
from uuid import uuid4

import boto3

import daggerml as dml

_loc_ = str(Path(__file__).parent / 'lambda')
STACK_NAME = 'test-batch'


@contextmanager
def cd(newpath):
    oldpath = os.getcwd()
    print('changing directory to', newpath)
    try:
        os.chdir(newpath)
        yield
    finally:
        os.chdir(oldpath)

def get_job_info(stack_name):
    cf_client = boto3.client('cloudformation')
    stack, = cf_client.describe_stacks(StackName=stack_name)['Stacks']
    outputs = {output['OutputKey']: output['OutputValue'] for output in stack['Outputs']}
    job_queue = outputs.get('JobQueue')
    job_definition = outputs.get('JobDefinition')
    if not job_queue or not job_definition:
        raise ValueError("JobQueue or JobDefinition not found in CloudFormation outputs")
    return job_queue, job_definition

def up(name):
    _id = f'{name}-{uuid4().hex[:8]}'
    with cd(_loc_):
        zip_file = f'{_id}.zip'
        subprocess.run([
            'zip', '-r', zip_file,
            '-x', '.git/*', '.dml/*', '__pycache__/*', 'tests/*', 'Makefile', 'lambda/*', '@',
            '.'
        ])
        boto3.client('s3').upload_file(zip_file, 'dml-test-misc2', f'test/{_id}.zip')
        os.remove(zip_file)
    job_queue, job_def = get_job_info(STACK_NAME)
    template = {
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "lambda.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            }
                        ]
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
                        "arn:aws:iam::aws:policy/AWSBatchFullAccess",
                        "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
                    ]
                }
            },
            "Dynamo": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [
                        {"AttributeName": "cache_key", "AttributeType": "S"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                    "KeySchema": [
                        {"AttributeName": "cache_key", "KeyType": "HASH"}
                    ],
                }
            },
            "Lambda": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"S3Bucket": 'dml-test-misc2', "S3Key": f'test/{_id}.zip'},
                    "Environment": {
                        "Variables": {
                            "DML_DYNAMO_TABLE": {"Ref": "Dynamo"},
                            "DML_JOB_QUEUE": job_queue,
                            "DML_JOB_DEF": job_def,
                        }
                    },
                    "Handler": 'entrypoint.handler',
                    "MemorySize": 128,
                    "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                    "Runtime": "python3.11",
                    "Timeout": 300,
                }
            }
        },
        "Outputs": {
            "LambdaArn": {
                "Description": "The lambda arn",
                "Value": {"Fn::GetAtt": ["Lambda", "Arn"]}
            },
        }
    }
    client = boto3.client('cloudformation')
    _ = client.create_stack(
        StackName=f'dml-{_id}',
        TemplateBody=json.dumps(template),
        TimeoutInMinutes=123,
        Capabilities=[
            'CAPABILITY_IAM','CAPABILITY_NAMED_IAM','CAPABILITY_AUTO_EXPAND',
        ],
        OnFailure='DELETE',
    )
    status = 'CREATE_IN_PROGRESS'
    desc = ''
    while status == 'CREATE_IN_PROGRESS':
        sleep(0.5)
        desc = client.describe_stacks(StackName=f'dml-{_id}')["Stacks"][0]
        status = desc['StackStatus']
    lambda_arn = None
    for obj in desc['Outputs']:
        if obj['OutputKey'] == 'LambdaArn':
            lambda_arn = obj['OutputValue']
            break
    assert isinstance(lambda_arn, str)
    print('finished building lambda:', lambda_arn, 'with status:', status, file=sys.stderr)
    return desc['StackName'], lambda_arn

def down(stack_name):
    client = boto3.client('cloudformation')
    client.delete_stack(StackName=stack_name)
