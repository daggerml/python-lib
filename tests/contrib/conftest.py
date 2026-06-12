from __future__ import annotations

import os
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import boto3
import pytest

import daggerml.api as api
from daggerml import Ref


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/contrib/" in str(item.fspath):
            item.add_marker(pytest.mark.contrib)
            if "/integration/" in str(item.fspath):
                item.add_marker(pytest.mark.slow)


def ref(namespace: str, ident: str) -> Ref:
    return Ref(f"{namespace}:{ident}")


@pytest.fixture(autouse=True)
def isolated_api_state(monkeypatch):
    monkeypatch.setattr(api, "_PROCESS_DEFAULT_DML", None)
    monkeypatch.setattr(
        api,
        "_SCOPED_DEFAULT_DML",
        ContextVar("daggerml_contrib_test_scoped_default_dml", default=api._NO_DEFAULT_DML),
    )
    monkeypatch.setattr(api, "_codecs", [(0, 1, api.NodeCodec()), (0, 2, api.MiscPyTypeCodec())])
    monkeypatch.setattr(api, "_plugins_loaded", True)


@pytest.fixture(autouse=True)
def clear_envvars():
    with patch.dict(os.environ):
        for key in list(os.environ):
            if key.startswith("AWS_") or key.startswith("DML_"):
                del os.environ[key]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        yield


@pytest.fixture(scope="session")
def aws_server():
    with patch.dict(os.environ):
        for key in list(os.environ):
            if key.startswith("AWS_"):
                del os.environ[key]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        try:
            yield {
                "endpoint": f"http://{host}:{port}",
                "envvars": {
                    "AWS_ACCESS_KEY_ID": "test",
                    "AWS_SECRET_ACCESS_KEY": "test",
                    "AWS_REGION": "us-east-1",
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ENDPOINT_URL": f"http://{host}:{port}",
                },
            }
        finally:
            server.stop()


@pytest.fixture
def remote_env(clear_envvars, aws_server):
    os.environ.update(aws_server["envvars"])
    os.environ["DML_REMOTE_ROOT"] = "s3://test-bucket/test-prefix"
    boto3.setup_default_session()
    yield aws_server["endpoint"]


@pytest.fixture
def s3_client(remote_env):
    return boto3.client("s3", endpoint_url=remote_env)


@pytest.fixture
def s3_bucket(s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    return "test-bucket"


@pytest.fixture
def refs():
    return SimpleNamespace(
        index=ref("index", "idx"),
        dag=ref("dag", "dag"),
        dag2=ref("dag", "dag2"),
        commit=ref("commit", "commit"),
        commit2=ref("commit", "commit2"),
        scalar=ref("node-literal", "scalar"),
        list=ref("node-literal", "list"),
        dict=ref("node-literal", "dict"),
        runnable=ref("node-literal", "runnable"),
        result=ref("node-literal", "result"),
        imported=ref("node-import", "imported"),
        fn=ref("node-fn", "fn"),
        argv=ref("node-argv", "argv"),
    )


@pytest.fixture
def fake_dml(refs):
    dml = SimpleNamespace(
        runtime=MagicMock(),
        dag=MagicMock(),
        show=MagicMock(),
        status=MagicMock(return_value={"repo": "ok"}),
    )
    dml.runtime.create.return_value = refs.index
    dml.runtime.describe.return_value = {"id": refs.index, "dag": refs.dag, "commit": refs.commit, "created": "now"}
    dml.runtime.put_literal.side_effect = lambda _index, value, name=None: {
        tuple: refs.list,
        list: refs.list,
        dict: refs.dict,
    }.get(type(value), refs.scalar)
    dml.runtime.get_node.return_value = refs.scalar
    dml.runtime.put_import.return_value = refs.imported
    dml.runtime.start_fn.return_value = refs.result
    dml.runtime.commit.return_value = refs.dag
    dml.dag.get_node.side_effect = lambda node, recursive=False: {
        refs.scalar: 42,
        refs.list: [1, 2],
        refs.dict: {"b": 2, "a": 1},
        refs.runnable: api.Runnable(target=api.Uri("daggerml:test"), kwargs={}, adapter=""),
        refs.result: True,
        refs.imported: "imported",
        refs.fn: "fn-result",
        refs.argv: ["argv"],
    }.get(node, "value")
    dml.dag.describe.return_value = {
        "id": refs.dag,
        "nodes": [refs.scalar, refs.list, refs.dict],
        "names": {"z": refs.dict, "a": refs.scalar},
        "error": None,
        "result": refs.result,
        "argv": refs.argv,
        "cache_key": None,
    }
    dml.dag.describe_node.return_value = {"id": refs.imported, "type": "ImportNode", "dag": refs.dag2}
    dml.show.return_value = {"dags": {"demo": refs.dag, "other": refs.dag2}}
    return dml


@pytest.fixture
def dag(fake_dml, refs):
    return api.Dag(dml=fake_dml, token=refs.index, name="demo", message="msg")
