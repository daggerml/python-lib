import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from textwrap import dedent
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

BUCKET = 'dml-test-misc2'
PREFIX = 'batch'
JOB_NAME_PREFIX = 'dml-misc'

def now():
    return datetime.now().astimezone(timezone.utc)

@dataclass
class Execution:
    cache_key: str
    dump: str
    cmd_tpl = """
        apt-get update -y
        apt-get install -y unzip curl
        curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
        unzip awscliv2.zip &>/dev/null
        ./aws/install
        rm -rf awscliv2.zip aws/
        mkdir /tmp/dml
        aws s3 cp "{lib_path}" /tmp/dml/dml_lib.zip
        (cd /tmp/dml && unzip dml_lib.zip && rm dml_lib.zip) &>/dev/null
        export PYTHONPATH=/tmp/dml
        export PATH="/tmp/dml/bin:$PATH"
        aws s3 cp {script_loc} /tmp/dml_script.py
        aws s3 cp {input_dump} /tmp/dag-input.json
        cat /tmp/dag-input.json | python3 /tmp/dml_script.py > /tmp/dag-output.json
        aws s3 cp /tmp/dag-output.json {output_dump}
    """

    @property
    def input_key(self):
        return f'{JOB_NAME_PREFIX}/{self.cache_key}/input.json'

    @property
    def output_key(self):
        return f'{JOB_NAME_PREFIX}/{self.cache_key}/output.json'

    @property
    def script_key(self):
        return f'{JOB_NAME_PREFIX}/{self.cache_key}/script.py'

    def start(self):
        s3 = boto3.client('s3')
        s3.upload_file('/var/task/add_one.py', BUCKET, self.script_key)
        s3.put_object(Bucket=BUCKET, Key=self.input_key, Body=self.dump.encode())
        cmd = dedent(self.cmd_tpl.format(
            lib_path=f"s3://{BUCKET}/test/python-lib.zip",
            input_dump=f's3://{BUCKET}/{self.input_key}',
            output_dump=f's3://{BUCKET}/{self.output_key}',
            script_loc=f's3://{BUCKET}/{self.script_key}',
        )).strip()

        response = boto3.client('batch').submit_job(
            jobName=f'{JOB_NAME_PREFIX}-{self.cache_key.replace("/", "-")}',
            jobQueue=os.getenv('DML_JOB_QUEUE'),
            jobDefinition=os.getenv('DML_JOB_DEF'),
            containerOverrides={
                'command': ["bash", "-c", cmd]
            }
        )
        return json.dumps({'job_id': response['jobId']})

    @staticmethod
    def poll(job_info):
        info = json.loads(job_info)
        resp = boto3.client('batch').describe_jobs(jobs=[info['job_id']])
        job, = resp['jobs']
        logger.info(json.dumps(job, indent=2, default=str))
        info['status'] = job['status']
        info['statusReason'] = job.get('statusReason')
        status = 'running'
        if job['status'] == 'SUCCEEDED':
            status = 'success'
        elif job['status'] == 'FAILED':
            status = 'fail'
        return status, json.dumps(info)

    def get_result(self, job_info):
        resp = boto3.client('s3').get_object(Bucket=BUCKET, Key=self.output_key)
        result = resp['Body'].read().decode()
        return result

def dynamo(ex):
    dyn = boto3.client('dynamodb')
    _id = uuid4().hex
    item = {'cache_key': ex.cache_key, 'status': 'reserved', 'info': _id}
    logger.info('checking item: %r', item)
    dynamo_table = os.getenv('DML_DYNAMO_TABLE')
    result = error = None
    try:
        dyn.put_item(
            TableName=dynamo_table,
            Item={k: {"S": str(v)} for k, v in item.items()},
            ConditionExpression='attribute_not_exists(cache_key)',
        )
        logger.info('successfully inserted item: %r', item)
        item['info'] = ex.start()
        item['status'] = 'running'
        dyn.put_item(
            TableName=dynamo_table,
            Item={k: {"S": v} for k, v in item.items()},
        )
        logger.info('updated item to: %r', item)
    except ClientError as e:
        logger.info('exception found (possibly ok) %r', item)
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        logger.info('exception was condition check... Getting updated item.')
        resp = dyn.get_item(
            TableName=dynamo_table,
            Key={
                'cache_key': {"S": ex.cache_key},
            },
        )
        item = {k: v['S'] for k, v in resp['Item'].items()}
        if item['status'] == 'running':
            logger.info('Updated item has status "running"... Polling now.')
            status, info = ex.poll(item['info'])
            if status != 'running':
                logger.info('setting status from %r to %r', item['status'], status)
                item['status'], item['info'] = status, info
                dyn.put_item(
                    TableName=dynamo_table,
                    Item={k: {"S": v} for k, v in item.items()},
                )
        if item['status'] == 'success':
            logger.info('found success status for %r', item)
            result = ex.get_result(item['info'])
        elif item['status'] == 'fail':
            logger.info('found fail status for %r', item)
            error = item['info']
        elif item['status'] not in ['reserved', 'running']:
            raise RuntimeError(f"{item['status']} ==> {item['info']}") from e
        logger.info(f'Finished polling... {type(result) = } *~* {type(error) = }')
    return result, error

def handler(event, context):
    logger.setLevel(logging.DEBUG)
    try:
        ex = Execution(event['cache_key'], event['dump'])
        result, error = dynamo(ex)
        return {'status': 0, 'result': result, 'error': error}
    except Exception as e:
        return {'status': 1, 'result': None, 'error': str(e)}
