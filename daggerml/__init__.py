import os
import boto3
import tarfile
import subprocess
from hashlib import md5
from tempfile import NamedTemporaryFile
from daggerml.util import Resource, Func, func, run, load, to_py
import pkg_resources  # part of setuptools


DML_ZONE = os.environ['DML_ZONE']
OUTPUT_BUCKET = 'daggerml-zone-{}-store'.format(DML_ZONE)

__version__ = pkg_resources.require('daggerml')[0].version
del pkg_resources

__all__ = ('Resource', 'Func', 'func', 'run', 'load', 'to_py', 'tar')


def tar(path):
    import sys
    def shell(*args, stdout=False, stderr=False):
        devnull = subprocess.DEVNULL
        stdout = subprocess.PIPE if stdout else devnull
        stderr = subprocess.PIPE if stderr else devnull
        proc = subprocess.run(args, stdout=stdout, stderr=stderr)
        if proc.returncode != 0:
            print(proc)
            raise RuntimeError(f"subprocess exit code: {proc.returncode}")
        if proc.stderr is not None:
            print(proc.stderr.decode())
        return proc.stdout
    if boto3 is None:
        raise RuntimeError('boto3 is required for the `tar` function')
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        # if not abspath, then it's relative to the calling function
        print('globals:', sys._getframe(1).f_globals)
        base = os.path.dirname(os.path.realpath(sys._getframe(1).f_globals['__file__']))
        path = os.path.normpath(os.path.join(base, path))
    if not os.path.isdir(path):
        raise ValueError('path %s is not a valid directory' % path)
    if not path.endswith('/'):
        path += '/'
    print('uploading', path)
    hash_script = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'extras/local-dir-hash.sh'
    )
    dir_hash = shell(hash_script, '-p', path, stdout=True).decode().strip()
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
    import sys
    if boto3 is None:
        raise RuntimeError('boto3 is required for the `tar` function')
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        # if not abspath, then it's relative to the calling function
        base = os.path.dirname(os.path.realpath(sys._getframe(1).f_globals['__file__']))
        path = os.path.normpath(os.path.join(base, path))
    if not os.path.isfile(path):
        raise ValueError('path %s is not a valid file' % path)
    print('uploading', path)
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


tar.__doc__ = \
    """create a tarball and return a dml.Resource

    Parameters
    ----------
    path : str
        The local path to the directory you want to tar

    Returns
    -------
    dml.Resource to the tarball on s3
    """
