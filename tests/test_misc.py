import os
import socket
import subprocess
import time
import unittest
from glob import glob
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory
from textwrap import dedent

import daggerml as dml
import daggerml.executor as dx
from tests.test_executor import MotoTestBase
from tests.util import DmlTestBase


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


def get_range_through_torch(dag):
    import torch
    print('getting expr...')
    n = dag.expr[1].value()
    dag.commit(torch.arange(n).tolist())


class TestMisc(DmlTestBase):

    def test_bytes(self):
        with TemporaryDirectory() as tmpd:
            cache = dx.Cache(f'{tmpd}/foo')
            assert cache.get() is None
            assert ls_r(tmpd) == ['.', 'foo.lock']
            data = {'x': 4}
            cache.put(data)
            assert cache.get() == data
            data = {'y': 8}
            cache.put(data)
            assert cache.get() == data

    def test_create_delete(self):
        with TemporaryDirectory() as tmpd:
            cache = dx.Cache(f'{tmpd}/bar')
            data = {'x': 4}
            cache.put(data)
            assert cache.get() == data
            with cache.lock():
                data = {'y': 8}
                cache.put(data)
                assert cache.get() == data
            cache.delete()
            assert cache.get() is None

    @unittest.skipIf(which("conda") is None, "conda is not available.")
    def test_conda_exec(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, get_range_through_torch, 'conda', 'torch')
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))

    @unittest.skipIf(which("conda") is None, "conda is not available.")
    def test_fn_indentation(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        def foo(dag):
            import torch
            n = dag.expr[1].value()
            dag.commit(torch.arange(n).tolist())
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, foo, 'conda', 'torch')
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))

class TestRemote(MotoTestBase):
    """spins up a local sshd server listening on a random port for tests"""
    @classmethod
    def setUpClass(cls):
        # Find a random free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            cls.port = s.getsockname()[1]

        cls.temp_dir = TemporaryDirectory()
        cls.home_dir = os.path.join(cls.temp_dir.name, "root")
        os.makedirs(cls.home_dir)
        cls.host_key_rsa = os.path.join(cls.temp_dir.name, 'ssh_host_rsa_key')
        subprocess.run(['ssh-keygen', '-t', 'rsa', '-f', cls.host_key_rsa, '-N', ''])
        cls.client_private_key = os.path.join(cls.temp_dir.name, 'id_rsa')
        cls.client_public_key = f"{cls.client_private_key}.pub"
        subprocess.run(['ssh-keygen', '-t', 'rsa', '-b', '2048', '-f', cls.client_private_key, '-N', ''])

        ssh_dir = os.path.join(cls.home_dir, '.ssh')
        os.makedirs(ssh_dir)
        authorized_keys = os.path.join(ssh_dir, 'authorized_keys')
        with open(cls.client_public_key, 'r') as pubkey_file:
            with open(authorized_keys, 'w') as auth_file:
                auth_file.write(pubkey_file.read())
        os.chmod(ssh_dir, 0o700)
        os.chmod(authorized_keys, 0o600)

        cls.sshd_config = os.path.join(cls.temp_dir.name, "sshd_config")
        with open(cls.sshd_config, 'w') as f:
            f.write(dedent(
                f"""
                Port {cls.port}
                ListenAddress 127.0.0.1
                HostKey {cls.host_key_rsa}
                PermitRootLogin yes
                PubkeyAuthentication yes
                PasswordAuthentication no
                AuthorizedKeysFile {authorized_keys}
                PidFile {cls.temp_dir.name}/sshd.pid
                """
            ).strip())

        cls.sshd_process = subprocess.Popen([
            '/usr/sbin/sshd', '-f', cls.sshd_config, '-D'
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for _ in range(10):  # Try for 10 seconds
            time.sleep(1)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(('127.0.0.1', cls.port))
                if result == 0:
                    print(f"SSHD started and listening on port {cls.port}")
                    break
        else:
            stdout, stderr = cls.sshd_process.communicate(timeout=5)
            print('SSHD output')
            print('=' * 80)
            print('stdout:')
            print(stdout)
            print('=' * 80)
            print('stderr:')
            print(stderr)
            print('=' * 80)
            raise RuntimeError(f"SSHD failed to start on port {cls.port}")
        cls.conn_params = {
            "host": "localhost",
            # "user": os.getlogin(),
            "port": cls.port,
            "pkey": cls.client_private_key,
            "opts": [
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ],
        }

    @classmethod
    def tearDownClass(cls):
        if cls.sshd_process:
            cls.sshd_process.terminate()
            cls.sshd_process.wait()
        cls.temp_dir.cleanup()

    def test_ssh(self):
        txt = 'hello world!'
        ssh = dx.Ssh(**self.conn_params)
        resp = ssh.run('cat', txt)
        assert resp == txt

    @unittest.skipIf(which("conda") is None, "conda is not available.")
    def test_conda_exec(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, get_range_through_torch, 'conda', 'torch',
                                conn_params=self.conn_params,
                                preambles=["source", "~/.local/conda/etc/profile.d/conda.sh;"])
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))

    @unittest.skipIf(which("hatch") is None, "hatch is not available.")
    def test_hatch_exec(self):
        from packaging.version import InvalidVersion, Version
        def is_valid_version_string(version_str):
            try:
                Version(version_str)
                return True
            except InvalidVersion:
                return False
        def foo(dag):
            import sklearn
            dag.commit(sklearn.__version__)
        repo_root = str(Path(__file__).parent.parent)
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, foo, 'hatch', 'other-test',
                                conn_params=self.conn_params,
                                preambles=[f"cd {repo_root};",
                                           "export PATH=$HOME/.local/bin:/opt/homebrew/bin:$PATH;"])
                resp = lx.run(dag, fn)
                vers = resp.get_result().value()
                assert is_valid_version_string(vers)
                # wrong env
                fn = lx.make_fn(dag, foo, 'hatch', 'test',
                                conn_params=self.conn_params,
                                preambles=[f"cd {repo_root};",
                                           "export PATH=$HOME/.local/bin:/opt/homebrew/bin:$PATH;"])
                with self.assertRaises(dml.Error):
                    resp = lx.run(dag, fn)
                    vers = resp.get_result().value()

    @unittest.skipIf(which("conda") is None, "conda is not available.")
    def test_fn_indentation(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        def foo(dag):
            import torch
            n = dag.expr[1].value()
            dag.commit(torch.arange(n).tolist())
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, foo, 'conda', 'torch',
                                conn_params=self.conn_params,
                                preambles=["source", "~/.local/conda/etc/profile.d/conda.sh;"])
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))
