#!/usr/bin/env python3
"""
* In general, methods ending with _ denote functions that take non Node arguments
  (and hence require compute). For example, building a tarball requires us to
  actually build the tarball before we can know the ID.
"""
import argparse
import json
import logging
import sys
from time import sleep
from urllib.parse import urlparse
from uuid import uuid4

import boto3

import daggerml as dml

logger = logging.getLogger(__name__)

def run(dag, expr, boto_session=boto3):
    expr = [dag.put(x) for x in expr]
    waiter = dag.start_fn(*expr, use_cache=False)
    resource, *_ = expr
    client = boto_session.client('lambda')
    resp = client.invoke(FunctionName=resource.value().id, Payload=json.dumps({'dump': waiter.dump}).encode())
    payload = json.loads(resp['Payload'].read().decode())
    dag.load_ref(payload["stdout"])
    return waiter.get_result()


def install(name, zip_resource):
    _id = f'{name}-{uuid4().hex[:8]}'
    parsed = urlparse(json.loads(zip_resource.value().id)['uri'])
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
                    "Code": {"S3Bucket": parsed.netloc, "S3Key": parsed.path[1:]},
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
    client.create_stack(
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
    dml.new(name, 'creating lambda function').commit(dml.Resource(json.dumps(resource_id)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('name')
    args = parser.parse_args()
    install(args.name)
