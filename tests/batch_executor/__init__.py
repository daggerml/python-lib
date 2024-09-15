import io
import json
import zipfile
from pathlib import Path
from time import sleep

import boto3

_loc_ = Path(__file__).parent
ALL_CAPS = ['CAPABILITY_IAM','CAPABILITY_NAMED_IAM','CAPABILITY_AUTO_EXPAND']

def cluster_json():
    js = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "AWS Batch cluster setup for running Python 3.11 scripts.",
        "Resources": {
            "VPC": {
                "Type": "AWS::EC2::VPC",
                "Properties": {
                    "CidrBlock": "10.0.0.0/16",
                    "EnableDnsSupport": True,
                    "EnableDnsHostnames": True,
                }
            },
            "Subnet": {
                "Type": "AWS::EC2::Subnet",
                "Properties": {
                    "VpcId": {
                        "Ref": "VPC"
                    },
                    "CidrBlock": "10.0.0.0/24",
                    "MapPublicIpOnLaunch": True,
                    "AvailabilityZone": {
                        "Fn::Select": [0, {"Fn::GetAZs": ""}]
                    },
                }
            },
            "InternetGateway": {
                "Type": "AWS::EC2::InternetGateway",
                "Properties": {}
            },
            "AttachGateway": {
                "Type": "AWS::EC2::VPCGatewayAttachment",
                "Properties": {
                    "VpcId": {"Ref": "VPC"},
                    "InternetGatewayId": {"Ref": "InternetGateway"}
                }
            },
            "RouteTable": {
                "Type": "AWS::EC2::RouteTable",
                "Properties": {
                    "VpcId": {"Ref": "VPC"},
                }
            },
            "Route": {
                "Type": "AWS::EC2::Route",
                "DependsOn": "AttachGateway",
                "Properties": {
                    "RouteTableId": {"Ref": "RouteTable"},
                    "DestinationCidrBlock": "0.0.0.0/0",
                    "GatewayId": {"Ref": "InternetGateway"}
                }
            },
            "SubnetRouteTableAssociation": {
                "Type": "AWS::EC2::SubnetRouteTableAssociation",
                "Properties": {
                    "SubnetId": {"Ref": "Subnet"},
                    "RouteTableId": {"Ref": "RouteTable"}
                }
            },
            "SecurityGroup": {
                "Type": "AWS::EC2::SecurityGroup",
                "Properties": {
                    "GroupDescription": "Allow SSH and all outbound traffic",
                    "VpcId": {"Ref": "VPC"},
                    "SecurityGroupIngress": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 22,
                            "ToPort": 22,
                            "CidrIp": "0.0.0.0/0"
                        }
                    ],
                    "SecurityGroupEgress": [
                        {
                            "IpProtocol": "-1",
                            "CidrIp": "0.0.0.0/0"
                        }
                    ]
                }
            },
            "InstanceRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "ec2.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            },
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "ecs-tasks.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            }
                        ]
                    },
                    "Path": "/",
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
                        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
                        "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
                    ]
                }
            },
            "InstanceProfile": {
                "Type": "AWS::IAM::InstanceProfile",
                "Properties": {
                    "Roles": [{"Ref": "InstanceRole"}],
                    "Path": "/"
                }
            },
            "BatchComputeEnvironment": {
                "Type": "AWS::Batch::ComputeEnvironment",
                "Properties": {
                    "ComputeEnvironmentName": "BatchComputeEnvironment",
                    "Type": "MANAGED",
                    "ComputeResources": {
                        "Type": "EC2",
                        "MinvCpus": 0,
                        "MaxvCpus": 16,
                        "DesiredvCpus": 2,
                        "InstanceTypes": [
                            "optimal"
                        ],
                        "Subnets": [{"Ref": "Subnet"}],
                        "SecurityGroupIds": [{"Ref": "SecurityGroup"}],
                        "InstanceRole": {"Fn::GetAtt": ["InstanceProfile", "Arn"]}
                    },
                    "ServiceRole": {"Fn::GetAtt": ["BatchServiceRole", "Arn"]}
                }
            },
            "BatchJobQueue": {
                "Type": "AWS::Batch::JobQueue",
                "Properties": {
                    "JobQueueName": "BatchJobQueue",
                    "State": "ENABLED",
                    "Priority": 1,
                    "ComputeEnvironmentOrder": [
                        {
                            "Order": 1,
                            "ComputeEnvironment": {"Ref": "BatchComputeEnvironment"}
                        }
                    ]
                }
            },
            "Repo": {
                "Type": "AWS::ECR::Repository",
                "Properties": {
                    "RepositoryName": "asdf",
                    "EmptyOnDelete": False,
                    "ImageTagMutability": "IMMUTABLE"
                }
            },
            "BatchServiceRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "batch.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            }
                        ]
                    },
                    "Path": "/",
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
                    ]
                }
            },
            "LambdaRole": {
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
                        "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
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
        },
        "Outputs": {
            "Dynamo": {"Value": {"Ref": "Dynamo"}},
            "LambdaRole": {"Value": {"Fn::GetAtt": ["LambdaRole", "Arn"]}},
            "JobQueue": {
                "Description": "The ARN of the Batch Job Queue",
                "Value": {"Ref": "BatchJobQueue"}
            },
            "RepoName": {
                "Description": "The name of the ECR Repo",
                "Value": {"Ref": "Repo"}
            },
            "RepoUri": {
                "Description": "The URI of the ECR Repo",
                "Value": {"Fn::GetAtt": ["Repo", "RepositoryUri"]}
            },
            # "LambdaArn": {
            #     "Description": "The lambda arn",
            #     "Value": {"Fn::GetAtt": ["Lambda", "Arn"]}
            # },
        }
    }
    return json.dumps(js)


def jobdef_json(image_uri):
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
                                    "Service": "ec2.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            },
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": "ecs-tasks.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                            },
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
                        "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
                    ]
                }
            },
            "JobDef": {
                "Type": "AWS::Batch::JobDefinition",
                "Properties": {
                    "JobDefinitionName": "aaron_job_def",
                    "Type": "container",
                    "ContainerProperties": {
                        "Image": image_uri,
                        "Vcpus": 1,
                        "Memory": 1024,
                        "JobRoleArn": {"Fn::GetAtt": ["Role", "Arn"]},
                    },
                    "RetryStrategy": {"Attempts": 1},
                }
            },
        },
        "Outputs": {
            "JobDef": {
                "Description": "The Job definition",
                "Value": {"Ref": "JobDef"}
            },
        }
    }
    return json.dumps(template)


def spin_up(name, template, capabilities):
    client = boto3.client('cloudformation')
    _ = client.create_stack(
        StackName=name,
        TemplateBody=template,
        TimeoutInMinutes=123,
        Capabilities=capabilities,
        OnFailure='DELETE',
    )
    status = 'CREATE_IN_PROGRESS'
    desc = ''
    while status == 'CREATE_IN_PROGRESS':
        sleep(0.5)
        desc = client.describe_stacks(StackName=name)["Stacks"][0]
        status = desc['StackStatus']
    return {x['OutputKey']: x['OutputValue'] for x in desc['Outputs']}


def get_code(fname):
    with open(_loc_/fname, 'r') as f:
        code = f.read()
    # Create a zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as z:
        z.writestr(fname, code)  # Use a valid filename
    zip_buffer.seek(0)
    return zip_buffer.read()

def up_cluster(bucket):
    resp = spin_up('cluster', cluster_json(), ALL_CAPS)
    # lambda functions are not implemented in moto v5.0.12
    lam_cli = (
        boto3.client('lambda')
        .create_function(
            FunctionName='my_lambda',
            Code={"ZipFile": get_code('lambda_entrypoint.py')},
            Environment={
                "Variables": {
                    "DML_DYNAMO_TABLE": resp.pop("Dynamo"),
                    "DML_S3_BUCKET": bucket,
                    "DML_S3_PREFIX": "tmp",
                }
            },
            Handler='lambda_entrypoint.handler',
            MemorySize=128,
            Role=resp.pop("LambdaRole"),
            Runtime="python3.11",
            Timeout=300,
        )
    )
    return dict(**resp, LambdaArn=lam_cli['FunctionArn'])

def up_jobdef(img):
    resp = spin_up('jobdef', jobdef_json(img), ALL_CAPS)
    return resp
