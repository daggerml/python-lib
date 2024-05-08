import subprocess
import tarfile
from glob import glob
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent

import boto3

import daggerml as dml
import daggerml.executor as ec2
from tests.util import DmlTestBase

_here_ = Path(__file__).parent
TEST_BUCKET = 'amn-dgr'
try:
    boto3.client('s3').list_objects_v2(Bucket=TEST_BUCKET)
    S3_ACCESS = True
except KeyboardInterrupt:
    raise
except Exception:
    S3_ACCESS = False


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f'{path}/**', recursive=True)]


class TestCore(DmlTestBase):

    def test_tar_hash(self):
        with NamedTemporaryFile() as f:
            h0 = ec2.make_tarball(_here_, f.name)
        with NamedTemporaryFile() as f:
            h1 = ec2.make_tarball(_here_, f.name)
        assert h0 == h1
        with NamedTemporaryFile() as f:
            h1 = ec2.make_tarball(_here_.parent, f.name)
        assert h0 != h1

    def test_tar_contents(self):
        with NamedTemporaryFile() as f:
            ec2.make_tarball(_here_, f.name)
            with TemporaryDirectory() as tmpd:
                with tarfile.open(f.name, 'r:gz') as tf:
                    tf.extractall(tmpd)
                assert ls_r(_here_) == ls_r(tmpd)

    def test_docker_build_n_run(self):
        # def docker_build(path, dkr_path=None, flags=()):
        res = {'foo': 'bar', 'baz': 12}
        dkr = ec2.docker_build(_here_.parent, dkr_path=_here_/'Dockerfile')
        # dkr = {'repository': 'dml', 'id': '5e96d885db6bd27d56097ecddff59ed4d0b6d493585dbea7030abf2718a02249'}
        with NamedTemporaryFile() as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write(dedent(f"""
                #!/usr/bin/env python3
                import json
                import sys

                import daggerml as dml

                with open(sys.argv[1], 'w') as f:
                    f.write(dml.to_json({res!r}))
                """).strip())
                f.flush()
                f.seek(0)
            subprocess.run(['chmod', '+x', tmpf.name])
            r2 = ec2.docker_run(dkr, tmpf.name)
        assert res == r2

class TestPorcelain(DmlTestBase):

    def test_tar(self):
        dag = self.new('test-dag0', 'this is a test')
        rsrc = dag.sh.tar_(_here_)
        with TemporaryDirectory() as tmpd:
            dag.sh.untar(rsrc, tmpd)
            assert ls_r(tmpd) == ls_r(_here_)

    # @unittest.skipUnless(S3_ACCESS, 'no s3 access')
    # def test_s3_tar(self):
    #     dag = self.new('test-dag0', 'this is a test')
    #     with TemporaryDirectory() as tmpd:
    #         rsrc = dag.sh.s3_tar(_here_, f's3://{TEST_BUCKET}/tar')
    #         dag.sh.s3_untar(rsrc, tmpd)
    #         assert ls_r(tmpd) == ls_r(_here_)

    def test_tmptar(self):
        dag = self.new('test-dag0', 'this is a test')
        sh = dag.sh
        tar = sh.tar_(_here_)
        assert isinstance(tar, dml.Node)
        with dag.sh.untar(tar) as tmpd:
            assert ls_r(tmpd) == ls_r(_here_)

    def test_docker_build_n_run(self):
        dag = self.new('test-dag0', 'this is a test')
        sh = dag.sh
        dkr = dag.dkr
        with self.assertLogs('daggerml.executor', 'INFO') as cm:
            tar = sh.tar_(_here_.parent)
        assert any(['committing result' in x for x in cm.output])
        img = dkr.build(tar, dkr_path='./tests/Dockerfile')
        assert dkr.exists(img)
        resp = dkr.run(img, [])
        res = {'foo': 'bar', 'baz': 12}
        # dkr = {'repository': 'dml', 'id': '5e96d885db6bd27d56097ecddff59ed4d0b6d493585dbea7030abf2718a02249'}
        with NamedTemporaryFile() as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write(dedent(f"""
                #!/usr/bin/env python3
                import json
                import sys

                import daggerml as dml

                with open(sys.argv[1], 'w') as f:
                    f.write(dml.to_json({res!r}))
                """).strip())
                f.flush()
                f.seek(0)
            subprocess.run(['chmod', '+x', tmpf.name])
            r2 = ec2.docker_run(dkr, tmpf.name)
        assert res == r2
