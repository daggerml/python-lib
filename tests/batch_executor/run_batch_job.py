import argparse
import json
import os
import subprocess
import textwrap
from contextlib import contextmanager
from inspect import getsource
from pathlib import Path
from uuid import uuid4

import boto3

import daggerml as dml

# The name of the CloudFormation stack
stack_name = 'test-batch'  # Replace with your CloudFormation stack name
_repo_root_ = str(Path(__file__).parent.parent.parent)
BUCKET = 'dml-test-misc2'

setup_cmd = """
    apt-get update -y &&
    apt-get install -y zip unzip curl &&
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip &&
    unzip awscliv2.zip &&
    ./aws/install &&
    rm -rf awscliv2.zip aws/ &&
    python3 -m ensurepip &&
    aws s3 cp 's3://{BUCKET}/test/{_id}.zip' /tmp/dml.zip &&
    unzip /tmp/dml.zip -d /tmp/dml-raw/ &&
    python3 '-m' 'pip' 'install' '-t' '/tmp/dml-install/' '/tmp/dml-raw/submodules/daggerml_cli' &&
    python3 '-m' 'pip' 'install' '-t' '/tmp/dml-install/' '/tmp/dml-raw/' &&
    find /tmp/dml-install/ -depth -type d -name __pycache__ -exec rm -rf {{}} \\; &&
    (cd /tmp/dml-install/ && zip -r ../dml-lib.zip .)
    aws s3 cp /tmp/dml-lib.zip 's3://{BUCKET}/test/python-lib.zip' &&
    python3 -c "print('Hello again AWS Batch!')"
"""
run_cmd = """
    apt-get update -y &&
    apt-get install -y unzip curl &&
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip &&
    unzip awscliv2.zip &&
    ./aws/install &&
    rm -rf awscliv2.zip aws/ &&
    aws s3 sync "s3://{BUCKET}/test/python-lib/" /tmp/dml/ &&
    export PYTHONPATH=/tmp/dml
    export PATH="/tmp/dml/bin:$PATH"
    python3 -c 'import daggerml as dml; print(f"{{dml.__version__ = }} ===> {{dml.__file__ = }} ===> _id = {_id}")'
"""

def get_job_info(stack_name):
    cf_client = boto3.client('cloudformation')
    stack, = cf_client.describe_stacks(StackName=stack_name)['Stacks']
    outputs = {output['OutputKey']: output['OutputValue'] for output in stack['Outputs']}
    job_queue = outputs.get('JobQueue')
    job_definition = outputs.get('JobDefinition')
    if not job_queue or not job_definition:
        raise ValueError("JobQueue or JobDefinition not found in CloudFormation outputs")
    return job_queue, job_definition

def fn(dag):
    return dag.commit(dag.expr[1])

@contextmanager
def cd(path):
    cwd = os.getcwd()
    print('changing directory to', path)
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(cwd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('name')
    parser.add_argument('--setup', action='store_true')
    args = parser.parse_args()

    _id = f'{args.name}-{uuid4().hex[:8]}'
    job_name = f'job-{_id}'
    job_queue, job_definition = get_job_info(stack_name)

    cmd = run_cmd
    if args.setup:
        cmd = setup_cmd 
        with cd(_repo_root_):
            zip_file = f'{_id}.zip'
            subprocess.run([
                'zip', '-r', zip_file,
                '-x', '.git/*', '.dml/*', '__pycache__/*', 'tests/*', 'Makefile',
                'batch-executor/*', 'tmp.output.json', '@',
                '.'
            ])
            boto3.client('s3').upload_file(zip_file, BUCKET, f'test/{_id}.zip')
            os.remove(zip_file)

    dag = dml.new('asdf', 'qwer')
    expr = [dml.Resource(json.dumps({'fn': getsource(fn), 'job_def': job_definition})), ]
    response = boto3.client('batch').submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            'command': [
                "bash", "-c", textwrap.dedent(cmd.format(BUCKET=BUCKET, _id=_id)).strip()
            ]
        }
    )
    print(f"Job submitted! Job ID: {response['jobId']}")
