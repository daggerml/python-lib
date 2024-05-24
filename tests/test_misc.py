import subprocess
import tarfile
from glob import glob
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent

import boto3
import pytest

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


def exclude_tests(x):
    if x.name.startswith('.'):  # no hidden files should be necessary for this
        return None
    if '__pycache__' in x.name:
        return None
    if x.name.startswith('submodule'):
        return None
    if x.name.startswith('tests/') and 'assets' not in x.name:
        return None
    return x


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

    @pytest.mark.slow
    def test_docker_build_n_run(self):
        res = {'foo': 'bar', 'baz': 12}
        dkr = ec2.docker_build(_here_.parent, dkr_path=_here_/'assets/Dockerfile')
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

class TestShExec(DmlTestBase):

    def test_tar(self):
        dag = self.new('test-dag0', 'this is a test')
        rsrc = dag.sh.tar_(_here_)
        with dag.sh.hydrate(x=rsrc) as tmpd:
            assert ls_r(f'{tmpd}/x/') == ls_r(_here_)

    def test_tmptar(self):
        dag = self.new('test-dag0', 'this is a test')
        sh = dag.sh
        tar = sh.tar_(_here_)
        assert isinstance(tar, dml.Node)
        with dag.sh.hydrate(x=tar) as tmpd:
            assert ls_r(f'{tmpd}/x/') == ls_r(_here_)

    def test_run_hatch(self):
        dag = self.new('test0', 'testing')
        def foopy(doc):
            from io import StringIO

            import pandas as pd
            print(doc)
            df = pd.read_csv(StringIO(doc))
            return df.sum().to_dict()

        csv = dedent("""
        a,b
        1,9
        2,8
        3,7
        4,6
        """).strip()
        res = dag.sh.run_hatch(foopy, csv, env='test-pandas')
        assert dag.get_value(res) == {'a': 10, 'b': 30}

    def test_bytes(self):
        dag = self.new('test', 'asdf')
        bts = b'testing 123'
        node = dag.sh.put_bytes_(bts)
        with dag.sh.hydrate(**{'x.bytes': node}) as tmpd:
            with open(f'{tmpd}/x.bytes', 'rb') as f:
                assert f.read() == bts

    def test_run_hatch_script(self):
        dag = self.new('test', 'foo')
        def fn(conf):
            from tempfile import TemporaryDirectory

            import mnist
            import numpy as np
            from ml_collections import ConfigDict
            config = ConfigDict(conf)
            with TemporaryDirectory(prefix='mnist-') as tmpd:
                result, loss = mnist.train_and_evaluate(config, tmpd)
            # params_bytes = serialization.to_bytes(result.params)
            return float(np.array(loss))
        config = {
            'num_epochs': 1,
            'batch_size': 20,
            'layer0_features': 32,
            'layer1_features': 64,
            'learning_rate': 0.01,
            'momentum': 0.9,
        }
        node = dag.sh.run_hatch(fn, config, mounts={'mnist.py': _here_/'assets/mnist_jax.py'}, env='test-jax')
        assert isinstance(dag.get_value(node), float)


class TestDkrExec(DmlTestBase):

    @pytest.mark.slow
    def test_build_n_run(self):
        dag = self.new('test-dag0', 'this is a test')
        sh = dag.sh
        dkr = dag.dkr
        with self.assertLogs('daggerml.executor', 'INFO') as cm:
            tar = sh.tar_(_here_.parent, filter_fn=exclude_tests)
        assert any(['committing result' in x for x in cm.output])
        img = dkr.build(tar, dkr_path='./tests/assets/Dockerfile')
        assert dkr.exists(img)
        def f(d):
            import pandas as pd
            series = pd.Series(d)
            return (series ** 2).to_dict()
        x = {'foo': 3, 'baz': 12}
        resp = dkr.run(img, f, x)
        assert dag.get_value(resp) == {k: v ** 2 for k, v in x.items()}
