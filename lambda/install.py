#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from time import sleep
from uuid import uuid4

import boto3

import daggerml as dml

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('name')
    args = parser.parse_args()
    _id = f'{args.name}-{uuid4().hex[:8]}'
    zip_file = f'lambda/{_id}.zip'
    subprocess.run(['zip', '-r', zip_file, '-x', '.git/*', '.dml/*', '__pycache__/*', 'tests/*', 'Makefile', 'lambda/*', '@', '.'])
    boto3.client('s3').upload_file(zip_file, 'dml-test-misc2', f'test/{_id}.zip')
    os.remove(zip_file)
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
                    "ManagedPolicyArns": ["arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]
                }
            },
            "Lambda": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"S3Bucket": 'dml-test-misc2', "S3Key": f'test/{_id}.zip'},
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
            }
        }
    }
    client = boto3.client('cloudformation')
    response = client.create_stack(
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
    resource_id = {
        "stack_id": desc['StackId'],
        "stack_name": desc['StackName'],
        "lambda_arn": lambda_arn
    }
    dml.new(args.name, 'creating lambda function').commit(dml.Resource(json.dumps(resource_id)))
