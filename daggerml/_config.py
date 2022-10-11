from os import getenv

LOCALSTACK_HOST = getenv('LOCALSTACK_HOST')
EDGE_PORT = getenv('EDGE_PORT', '4566')
AWS_LOCALSTACK_ENDPOINT = None

DML_ZONE = getenv('DML_ZONE', 'prod')
DML_REGION = getenv('DML_REGION', 'us-west-2')
DML_API_ENDPOINT = getenv('DML_API_ENDPOINT')

if DML_API_ENDPOINT is None:
    DML_API_ENDPOINT = 'https://api.{}-{}.daggerml.com'.format(DML_ZONE, DML_REGION)

if LOCALSTACK_HOST is not None:
    AWS_LOCALSTACK_ENDPOINT = 'http://{}:{}'.format(LOCALSTACK_HOST, EDGE_PORT)
