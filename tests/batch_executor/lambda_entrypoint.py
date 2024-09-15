import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from textwrap import dedent
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger('daggerml.lambda.entrypoint')

BUCKET = os.environ["DML_S3_BUCKET"]
PREFIX = os.getenv("DML_S3_PREFIX", "")
MOTO_URL = os.environ.get("MOTO_HTTP_ENDPOINT")
MOTO_PORT = os.environ.get("MOTO_PORT")
BOTO_KW = {} if MOTO_PORT is None else {"endpoint_url": f'http://localhost:{MOTO_PORT}'}

def now():
    return datetime.now().astimezone(timezone.utc)

@dataclass
class Execution:
    cache_key: str
    dump: str
    data: str
    cmd_tpl = """
        aws s3 cp {script_loc} /tmp/dml_script.py
        aws s3 cp {input_dump} /tmp/dag-input.json
        cat /tmp/dag-input.json | python3 /tmp/dml_script.py > /tmp/dag-output.json
        aws s3 cp /tmp/dag-output.json {output_dump}
    """

    @property
    def input_key(self):
        return f'{PREFIX}/{self.cache_key}/input.json'

    @property
    def output_key(self):
        return f'{PREFIX}/{self.cache_key}/output.json'

    def start(self):
        s3 = boto3.client('s3', **BOTO_KW)
        s3.put_object(Bucket=BUCKET, Key=self.input_key, Body=self.dump.encode())
        job_queue, job_def, script = json.loads(self.data)
        cmd = dedent(self.cmd_tpl.format(
            input_dump=f's3://{BUCKET}/{self.input_key}',
            output_dump=f's3://{BUCKET}/{self.output_key}',
            script_loc=script,
        )).strip()

        response = boto3.client('batch', **BOTO_KW).submit_job(
            jobName=f'{PREFIX}-{self.cache_key.replace("/", "-")}',
            jobQueue=job_queue,
            jobDefinition=job_def,
            containerOverrides={
                'command': ["bash", "-c", cmd]
            }
        )
        return json.dumps({'job_id': response['jobId']})

    @staticmethod
    def poll(job_info):
        info = json.loads(job_info)
        resp = boto3.client('batch', **BOTO_KW).describe_jobs(jobs=[info['job_id']])
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
        resp = boto3.client('s3', **BOTO_KW).get_object(Bucket=BUCKET, Key=self.output_key)
        result = resp['Body'].read().decode()
        return result

def dynamo(ex):
    dyn = boto3.client('dynamodb', **BOTO_KW)
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
        ex = Execution(**event)
        result, error = dynamo(ex)
        return {'status': 0, 'result': result, 'error': error}
    except Exception as e:
        return {'status': 1, 'result': None, 'error': str(e)}
