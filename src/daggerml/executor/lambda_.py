import json
import logging

import boto3

import daggerml as dml

logger = logging.getLogger(__name__)

def run(dag, expr, boto_session=boto3):
    expr = [dag.put(x) for x in expr]
    waiter = dag.start_fn(*expr)
    resource, *_ = expr
    resource_info = json.loads(resource.value().id)
    lambda_arn = resource_info['lambda_arn']

    def lambda_update_fn(cache_key, dump):
        resp = boto_session.client('lambda').invoke(
            FunctionName=lambda_arn,
            Payload=json.dumps({'dump': dump, 'cache_key': cache_key}).encode()
        )
        payload = json.loads(resp['Payload'].read().decode())
        if payload['status'] != 0:
            raise dml.Error(payload['error'])
        return payload['result']
    waiter = dml.FnUpdater.from_waiter(waiter, lambda_update_fn)
    waiter.update()
    return waiter
