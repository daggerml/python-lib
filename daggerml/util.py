import os
import sys
import json
import tarfile
import logging
from hashlib import md5
from tempfile import NamedTemporaryFile
import subprocess
from http.client import HTTPConnection, HTTPSConnection
try:
    import boto3
except ImportError:
    boto3 = None
from daggerml._types import Resource
from daggerml.exceptions import ApiError


logger = logging.getLogger(__name__)


###########################################################################
# AWS
###########################################################################

DML_ZONE = os.getenv('DML_ZONE', 'prod')
DML_GID = os.getenv('DML_GID', 'test-A')
DML_REGION = os.getenv('DML_REGION', 'us-west-2')
OUTPUT_BUCKET = 'daggerml-zone-{}-store'.format(DML_ZONE)


def api(op, **kwargs):
    try:
        if 'DML_LOCAL_DB' in os.environ:
            conn = HTTPConnection("localhost", 8081)
        else:
            conn = HTTPSConnection(f"api.{DML_ZONE}-{DML_REGION}.daggerml.com")
        if 'gid' not in kwargs:
            kwargs['gid'] = DML_GID
        headers = {'content-type': 'application/json', 'accept': 'application/json'}
        conn.request('POST', '/', json.dumps(dict(op=op, **kwargs)), headers)
        resp = conn.getresponse()
        if resp.status != 200:
            raise ApiError(f'{resp.status} {resp.reason}')
        resp = json.loads(resp.read())
        if resp['status'] != 'ok':
            err = resp['error']
            if err['context']:
                logger.error('api error: %s', '\n'.join(err['context']))
            raise ApiError(f'{err["code"]}: {err["message"]}')
        return resp['result']
    except KeyboardInterrupt:
        raise
    except ApiError:
        raise
    except Exception as e:
        raise ApiError(f'{e.__class__.__name__}: {str(e)}')


def commit_node(node_id, token, datum_id):
    return api(
        'commit_node',
        node_id=node_id,
        token=token,
        datum_id=datum_id
    )


def fail_node(node_id, token, err_msg):
    return api(
        'fail_node',
        node_id=node_id,
        token=token,
        error={'message': err_msg}
    )


def get_datum(datum_id, gid=DML_GID):
    return api('get_datum', id=datum_id, gid=gid)


def upsert_datum(value, type):
    return api('upsert_datum', value=value, type=type)


def _shell(*args, stdout=False, stderr=False):
    """run a shell command... Used internally"""
    devnull = subprocess.DEVNULL
    stdout = subprocess.PIPE if stdout else devnull
    stderr = subprocess.PIPE if stderr else devnull
    proc = subprocess.run(args, stdout=stdout, stderr=stderr)
    if proc.stderr is not None:
        logger.info('shell process has stderr: %s',
                    proc.stderr.decode())
    if proc.returncode != 0:
        logger.error('error in shell command: %s -- error: %s',
                     json.dumps(args), str(proc))
        raise RuntimeError(f"subprocess exit code: {proc.returncode}")
    return proc.stdout


def tar(path):
    """create a tarball and return a dml.Resource

    Parameters
    ----------
    path : str
        The local path to the directory you want to tar

    Returns
    -------
    dml.Resource to the tarball on s3
    """

    if boto3 is None:
        raise RuntimeError('boto3 is required for the `tar` function')
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        # if not abspath, then it's relative to the calling function
        base = os.path.dirname(os.path.realpath(sys._getframe(1).f_globals['__file__']))
        path = os.path.normpath(os.path.join(base, path))
    if not os.path.isdir(path):
        raise ValueError('path %s is not a valid directory' % path)
    if not path.endswith('/'):
        path += '/'
    logger.info('uploading %s', path)
    hash_script = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'extras/local-dir-hash.sh'
    )
    dir_hash = _shell(hash_script, '-p', path, stdout=True).decode().strip()
    s3_key = 'datum/s3-upload/%s/data.tar.gz' % dir_hash
    with NamedTemporaryFile(dir='/tmp/', suffix='.tar.gz', prefix='dml-s3-upload') as f:
        with tarfile.open(f.name, 'w:gz') as tar:
            tar.add(path, arcname=os.path.sep)
        boto3.client('s3').put_object(
            Body=f.read(), Bucket=OUTPUT_BUCKET, Key=s3_key
        )
    return Resource(
        's3-blob',
        {'uri': 's3://%s/%s' % (OUTPUT_BUCKET, s3_key)}
    )


def upload_file(path):
    if boto3 is None:
        raise RuntimeError('boto3 is required for the `tar` function')
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        # if not abspath, then it's relative to the calling function
        base = os.path.dirname(os.path.realpath(sys._getframe(1).f_globals['__file__']))
        path = os.path.normpath(os.path.join(base, path))
    if not os.path.isfile(path):
        raise ValueError('path %s is not a valid file' % path)
    logger.info('uploading %s', path)
    with open(path, 'rb') as f:
        txt = f.read()
        fhash = md5(txt).hexdigest()
        s3_key = 'datum/s3-upload/%s/%s' % (fhash, os.path.basename(path))
        boto3.client('s3').put_object(
            Body=txt, Bucket=OUTPUT_BUCKET, Key=s3_key
        )
    return Resource(
        's3-blob',
        {'uri': 's3://%s/%s' % (OUTPUT_BUCKET, s3_key)}
    )
