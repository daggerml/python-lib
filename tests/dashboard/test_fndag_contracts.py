import json

import pytest

from daggerml._core.db import Ref
from daggerml._core.types import Runnable, Uri
from daggerml.dashboard.read_model import DashboardReadModel, ScriptReadError


def test_dash_remote_001__dashboard_accepts_the_current_remote_descriptor(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    descriptor = {
        "schema": 0,
        "hash": "sha256",
        "layout": "one-project-cas+refs+split-execution",
        "refs_prefix": "refs",
        "io_prefix": "io",
        "cas_prefix": "cas/sha256",
        "execution_prefix": "../exec",
    }

    class Dml:
        class Config:
            @staticmethod
            def show():
                return {"remote": {"root": "s3://bucket/project"}}

        config = Config()

    class Client:
        @staticmethod
        def get_object(*, Bucket, Key):
            assert (Bucket, Key) == ("bucket", "project/dml/dml.json")
            return {"Body": type("Body", (), {"read": staticmethod(lambda: json.dumps(descriptor).encode())})()}

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml(), s3_client_factory=Client)

    model._require_remote_descriptor()
    assert model._remote_descriptor_verified is True


def test_dash_fndag_001__fndag_preserves_dml_execution_runtime_argv_and_output_boundaries(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        def read_execution_record(self, _execution_id):
            return {"execution_id": "run", "cache_key": "cache", "created_at": 10, "updated_at": 15}

        def describe(self, _index):
            return {"id": Ref("index:run"), "dag": Ref("dag:result")}

        def get_argv(self, _index):
            return Ref("node:argv")

    class Dag:
        def get_node(self, _argv, *, recursive):
            assert recursive is True
            return [Ref("node:input"), {"literal": 2}]

    fake_dml = type("Dml", (), {"runtime": Runtime(), "dag": Dag()})()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: fake_dml)
    model._require_remote_descriptor = lambda: None
    model.execution = lambda _execution_id: {"launch_state": {}, "resources": {"kind": "script"}}

    result = model.fndag("run")

    assert result["cache_key"] == "cache"
    assert result["argv"]["ref"] == "node:argv"
    assert result["argv"]["inputs"][0]["href"] == "/api/v1/nodes/node:input"
    assert result["output"] == {"ref": "dag:result", "href": "/api/v1/dags/dag:result"}
    assert result["timing"]["duration_seconds"] == 5


def test_dash_fndag_002__fndag_accepts_runtime_refs_from_the_runs_list(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        def read_execution_record(self, _execution_id):
            return {"execution_id": "run", "created_at": 1, "updated_at": 2}

        def describe(self, index):
            assert index == Ref("index:run")
            return {"dag": Ref("dag:result")}

        def get_argv(self, index):
            assert index == Ref("index:run")
            return Ref("node:argv")

    class Dag:
        def get_node(self, _argv, *, recursive):
            assert recursive is True
            return []

    fake_dml = type("Dml", (), {"runtime": Runtime(), "dag": Dag()})()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: fake_dml)
    model._require_remote_descriptor = lambda: None
    model.execution = lambda execution_id: {"execution_id": execution_id, "launch_state": {}, "resources": None}

    result = model.fndag("index:run")

    assert result["script"]["href"] == "/api/v1/executions/run/script"


def test_dash_fndag_003__completed_function_execution_resolves_its_cached_dag(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        def read_execution_record(self, _execution_id):
            return {"execution_id": "run", "cache_key": "cache", "created_at": 1, "updated_at": 2}

        def describe(self, _index):
            raise RuntimeError("completed function execution is not a local index")

    class Dag:
        def describe(self, dag):
            assert dag == Ref("dag:result")
            return {"argv": Ref("node:argv")}

        def get_node(self, _argv, *, recursive):
            assert recursive is True
            return []

    class Cache:
        def get(self, cache_key):
            assert cache_key == "cache"
            return Ref("dag:result")

    fake_dml = type("Dml", (), {"runtime": Runtime(), "dag": Dag(), "cache": Cache()})()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: fake_dml)
    model._require_remote_descriptor = lambda: None
    model.execution = lambda _execution_id: {"launch_state": {}, "resources": None}

    result = model.fndag("run")

    assert result["runtime"] is None
    assert result["output"] == {"ref": "dag:result", "href": "/api/v1/dags/dag:result"}


def test_dash_fndag_004__fn_node_and_context_dag_expose_navigation_and_runnable_resources(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    fn = Ref("node-fn:call")
    context = Ref("dag:context")
    argv = Ref("node-argv:args")

    class Dag:
        def describe_node(self, node):
            if node == fn:
                return {"id": fn, "type": "FnNode", "dag": context, "argv": [Ref("node-literal:runnable")]}
            assert node == argv
            return {"id": argv, "type": "ArgvNode"}

        def describe(self, dag):
            assert dag == context
            return {
                "id": context,
                "nodes": [argv],
                "names": {},
                "argv": argv,
                "result": None,
                "error": None,
                "cache_key": "cache-1",
            }

        def get_node(self, node, *, recursive):
            if node == fn:
                return "result"
            assert node == argv and recursive is True
            return [
                Runnable(
                    target=Uri("script"),
                    adapter="dml-local-adapter",
                    kwargs={"fn_name": "train", "script_uri": "s3://bucket/root/train.py"},
                )
            ]

    fake_dml = type("Dml", (), {"dag": Dag()})()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: fake_dml)

    node = model.node(fn.to)
    dag = model.dag(context.to)

    assert node["context_dag"] == {"ref": context.to, "href": f"/api/v1/dags/{context.to}"}
    assert node["function"]["cache_key"] == "cache-1"
    assert node["value_kind"] == "value"
    assert node["value_type"] == "str"
    assert node["function"]["runnable"]["entrypoint"]["details"]["fn_name"] == "train"
    assert dag["function"]["runnable"]["script"]["href"] == f"/api/v1/function-dags/{context.to}/script"


def test_dash_fndag_005__function_context_uses_only_argv_zero_as_the_applied_runnable(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    context = Ref("dag:context")
    argv = Ref("node-argv:args")
    script = Runnable(
        target=Uri("script"),
        adapter="dml-local-adapter",
        kwargs={"fn_name": "summarize", "script_uri": "s3://bucket/root/summarize.py"},
    )
    nested = Runnable(target=Uri("daggerml:dict"), kwargs={"prepop": {"summarizer": script}})
    applied = Runnable(target=Uri("ssh"), sub=script, kwargs={"host": "worker"})

    class Dag:
        def describe(self, dag):
            assert dag == context
            return {"argv": argv, "cache_key": "cache-2", "names": {}}

        def get_node(self, node, *, recursive):
            assert node == argv and recursive is True
            return [applied, nested, script]

    fake_dml = type("Dml", (), {"dag": Dag()})()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: fake_dml)

    function = model._function_context(context)

    assert function is not None
    assert function["runnable"]["stack"]["kind"] == "ssh"
    assert function["runnable"]["stack"]["sub"]["kind"] == "script"
    assert function["runnable"]["entrypoint"]["details"]["fn_name"] == "summarize"
    assert function["runnable"]["script"]["state"] == "available"


def test_dash_fndag_005a__runnable_value_and_prepopulation_are_explicit_and_bounded(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    node_ref = Ref("node-literal:runnable")
    runnable = Runnable(
        target=Uri("script"),
        kwargs={
            "script_uri": Uri("s3://bucket/root/value.py"),
            "prepop": {"secret": {"token": "do-not-serialize"}},
        },
    )

    class Dag:
        @staticmethod
        def describe_node(node):
            assert node == node_ref
            return {"id": node, "type": "LiteralNode"}

        @staticmethod
        def get_node(node, *, recursive):
            assert node == node_ref and recursive is False
            return runnable

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: type("Dml", (), {"dag": Dag()})())

    payload = model.node(node_ref.to)

    assert payload["value_kind"] == "runnable"
    assert payload["value_type"] == "Runnable"
    assert payload["value_runnable"]["prepopulated"] == [
        {"name": "secret", "type": "dict", "node": None}
    ]
    assert payload["value_runnable"]["script"] == {
        "state": "available",
        "uri": "s3://bucket/root/value.py",
        "href": f"/api/v1/nodes/{node_ref.to}/value/script",
    }
    assert "do-not-serialize" not in str(payload["value_runnable"])


def test_dash_fndag_005b__script_failures_have_stable_cause_specific_codes(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Config:
        root = None

        def show(self):
            return {"remote": {"root": self.root}}

    config = Config()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: type("Dml", (), {"config": config})())

    with pytest.raises(ScriptReadError) as non_script:
        model._read_script_runnable(Runnable(target=Uri("ssh")), max_bytes=100)
    assert non_script.value.code == "not-python-script"

    with pytest.raises(ScriptReadError) as missing_uri:
        model._read_script_runnable(Runnable(target=Uri("script")), max_bytes=100)
    assert missing_uri.value.code == "script-uri-unavailable"

    captured = {}
    original_read_script_uri = model._read_script_uri

    def read_script_uri(uri, *, max_bytes):
        captured.update(uri=uri, max_bytes=max_bytes)
        return {"source": "ok"}

    model._read_script_uri = read_script_uri
    assert model._read_script_runnable(
        Runnable(target=Uri("script"), kwargs={"script_uri": Uri("s3://bucket/root/script.py")}),
        max_bytes=321,
    ) == {"source": "ok"}
    assert captured == {"uri": "s3://bucket/root/script.py", "max_bytes": 321}
    model._read_script_uri = original_read_script_uri

    with pytest.raises(ScriptReadError) as unconfigured:
        model._read_script_uri("s3://bucket/root/script.py", max_bytes=100)
    assert unconfigured.value.code == "remote-unconfigured"

    config.root = "s3://bucket/root"
    with pytest.raises(ScriptReadError) as forbidden:
        model._read_script_uri("s3://other/root/script.py", max_bytes=100)
    assert forbidden.value.code == "script-outside-remote-root"


def test_dash_fndag_006__completed_function_dag_reads_logs_by_persisted_cache_key(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    context = Ref("dag:context")
    argv = Ref("node-argv:args")

    class Dag:
        def describe(self, dag):
            assert dag == context
            return {"argv": argv, "cache_key": "durable-cache"}

        def get_node(self, node, *, recursive):
            assert node == argv and recursive is True
            return [Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={})]

    class Logs:
        def get_log_events(self, **kwargs):
            assert kwargs["logStreamName"] == "/run/durable-cache/stdout"
            return {"events": [{"timestamp": 1, "message": "completed"}]}

    fake_dml = type("Dml", (), {"dag": Dag()})()
    model = DashboardReadModel(
        tmp_path,
        dml_factory=lambda **_kwargs: fake_dml,
        cloudwatch_client_factory=Logs,
    )

    result = model.function_dag_logs("dag:context", "stdout")

    assert result["events"] == [{"timestamp": 1, "message": "completed"}]


def test_dash_dag_006__dag_collection_includes_accurate_counts_and_terminal_status(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    ready = Ref("dag:ready")
    failed = Ref("dag:failed")

    class Dag:
        def describe(self, dag):
            return {
                "nodes": [Ref("node-literal:one"), Ref("node-literal:two")] if dag == ready else [],
                "error": None if dag == ready else Ref("error:failed"),
            }

    class Dml:
        dag = Dag()

        def show(self, revision):
            assert revision == "HEAD"
            return {"id": Ref("commit:head"), "dags": {"ready": ready, "failed": failed}}

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())

    result = model.dags()

    assert result["items"] == [
        {
            "name": "failed",
            "id": failed.to,
            "commit": "commit:head",
            "tags": [],
            "node_count": 0,
            "status": "error",
        },
        {
            "name": "ready",
            "id": ready.to,
            "commit": "commit:head",
            "tags": [],
            "node_count": 2,
            "status": "ready",
        },
    ]
