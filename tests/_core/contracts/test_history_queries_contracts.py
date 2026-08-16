from __future__ import annotations

import sys
from types import ModuleType

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core import DmlRepoError
from daggerml._core.db import Ref
from daggerml._core.head import Head
from tests._core.helpers import NoopExecutionState, commit_literal_dag, local_index_ops, make_local_dml


def test_status_reports_attached_head_branch_list_and_live_indexes(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    dml.runtime.create()

    assert dml.status() == {
        "mode": "attached",
        "branch": "main",
        "commit": None,
        "branches": [],
        "upstream": None,
        "num_indexes": 1,
        "ahead": None,
        "behind": None,
    }


def test_runtime_create_accepts_execution_ref_and_delegates_id(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    execution = Ref("index:exec-1")
    calls = []

    class Ops:
        def create(self, author, commit, cache_key=None, execution_id=None, *, db):
            calls.append((author, commit, cache_key, execution_id, db))
            return execution

    monkeypatch.setattr(dml_mod, "_index_ops", lambda _dml: Ops())

    assert dml.runtime.create(cache_key="cache-1", execution=execution) == execution
    assert calls == [("tester", None, "cache-1", "exec-1", dml._db)]


@pytest.mark.parametrize("execution", ["exec-1", "index:exec-1", Ref("dag:exec-1")])
def test_runtime_create_rejects_non_index_execution_identity(tmp_path, monkeypatch, execution) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(TypeError, match="runtime Ref"):
        dml.runtime.create(cache_key="cache-1", execution=execution)


def test_runtime_create_requires_paired_cache_and_execution(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(DmlRepoError, match="both cache_key and execution"):
        dml.runtime.create(cache_key="cache-1")
    with pytest.raises(DmlRepoError, match="both cache_key and execution"):
        dml.runtime.create(execution=Ref("index:exec-1"))


def test_status_reports_detached_head_without_changing_branch_list(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit = commit_literal_dag(dml, "train", 1)

    status = dml.checkout("HEAD")

    assert status == {
        "mode": "detached",
        "branch": None,
        "commit": commit,
        "branches": ["main"],
        "upstream": None,
        "num_indexes": 0,
        "ahead": None,
        "behind": None,
    }


def test_status_reports_ahead_and_behind_relative_to_fetched_remote_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1, message="base")
    commit_literal_dag(dml, "local", 2, message="local")

    # Simulate the last fetched remote-tracking branch still pointing at base.
    from daggerml._core.head import Head

    head = Head(str(tmp_path))
    head.create_remote_tracking_ref("main", base)
    head.set_upstream("main", "main")

    status = dml.status()

    assert status["ahead"] == 1
    assert status["behind"] == 0


def test_status_reports_diverged_counts_against_fetched_remote_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1, message="base")

    from daggerml._core.head import Head

    head = Head(str(tmp_path))
    head.create_local_ref("feature", base)
    dml.checkout("feature")
    commit_literal_dag(dml, "feature-only", 2, message="feature-only")
    dml.checkout("main")
    remote_tip = commit_literal_dag(dml, "remote-only", 3, message="remote-only")

    head.create_remote_tracking_ref("feature", remote_tip)
    head.set_upstream("feature", "feature")
    dml.checkout("feature")

    status = dml.status()

    assert status["ahead"] == 1
    assert status["behind"] == 1


def test_log_show_and_diff_use_parent_relative_history_by_default(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1, message="train-v1")
    second = commit_literal_dag(dml, "eval", 2, message="eval-v1")

    diff = dml.diff(second)
    show = dml.show(second)
    log = dml.log(limit=2)["commits"]

    assert diff == {"added": {"eval": show["dags"]["eval"]}, "removed": {}, "modified": {}}
    assert show["diff"] == diff
    assert show["message"] == "eval-v1"
    assert "dag" not in show
    assert set(show["dags"]) == {"train", "eval"}
    assert [entry["message"] for entry in log] == ["eval-v1", "train-v1"]
    assert all("dag" not in entry for entry in log)


def test_diff_accepts_explicit_base_for_modified_dag_name(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "train", 1, message="train-v1")
    second = commit_literal_dag(dml, "train", 2, message="train-v2")

    diff = dml.diff(second, relative_to=first)

    assert diff == {
        "added": {},
        "removed": {},
        "modified": {"train": (dml.show(first)["dags"]["train"], dml.show(second)["dags"]["train"])},
    }


def test_runtime_commit_without_name_returns_dag_and_leaves_history_unchanged(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = dml.status()["commit"]
    index = dml.runtime.create()
    node = dml.runtime.put_literal(index, 1, name="value")

    dag_ref = dml.runtime.commit(index, node, message="unnamed")

    assert dag_ref.ns() == "dag"
    assert dml.status()["commit"] == base
    assert dml.runtime.list() == []


def test_runtime_describe_and_list_include_commit_shaped_payload_plus_dag(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = dml.status()["commit"]
    index = dml.runtime.create()

    described = dml.runtime.describe(index)
    listed = dml.runtime.list()

    assert described["id"] == index
    assert described["parents"] == ([] if base is None else [base])
    assert described["author"] == "tester"
    assert described["message"] == ""
    assert described["dag"].ns() == "dag"
    assert described["tree"].ns() == "tree"

    assert listed == [described]


def test_runtime_describe_graph_accepts_explicit_roots(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()

    graph = dml.runtime.describe_graph(index)

    assert graph["roots"] == [index.id()]
    assert graph["nodes"][index.id()]["execution_id"] == index.id()
    assert graph["nodes"][index.id()]["spawned"] == []
    assert graph["nodes"][index.id()]["children"] == []


def test_runtime_describe_graph_defaults_to_open_local_indexes(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = dml.runtime.create()
    second = dml.runtime.create()

    graph = dml.runtime.describe_graph()

    assert set(graph["roots"]) == {first.id(), second.id()}
    assert set(graph["nodes"]) == {first.id(), second.id()}


def test_runtime_read_execution_record_accepts_ref_and_returns_raw_payload(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()
    record = {
        "execution_id": index.id(),
        "cache_key": None,
        "lifecycle": "running",
        "updated_at": 10,
        "created_at": 9,
        "spawned_execution_ids": ["child-1"],
        "child_execution_ids": ["child-0"],
        "cancellation_requested_by": "tester",
    }
    state = NoopExecutionState()
    state.create_execution_record(record)
    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: state)

    assert dml.runtime.read_execution_record(index) == record


def test_runtime_read_execution_record_rejects_execution_id_strings(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    state = NoopExecutionState()
    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: state)

    for execution in ("exec-2", "index:exec-2"):
        with pytest.raises(TypeError, match="runtime Ref"):
            dml.runtime.read_execution_record(execution)
    assert state.records == {}


def test_runtime_read_execution_record_surfaces_missing_record_error(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    state = NoopExecutionState()
    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: state)

    with pytest.raises(DmlRepoError, match="No execution record found for execution_id: missing"):
        dml.runtime.read_execution_record(Ref("index:missing"))


def test_runtime_read_launch_state_delegates_exact_execution_id(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    calls = []

    class State:
        def read_launch_state(self, execution_id):
            calls.append(execution_id)
            return {"job_id": "j1"} if execution_id == "exec-1" else None

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    assert dml.runtime.read_launch_state(Ref("index:exec-1")) == {"job_id": "j1"}
    assert dml.runtime.read_launch_state(Ref("index:missing")) is None
    assert calls == ["exec-1", "missing"]


def test_runtime_read_launch_state_rejects_execution_id_strings_before_delegation(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    calls = []

    class State:
        def read_launch_state(self, execution_id):
            calls.append(execution_id)

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    for execution in ("exec-1", "index:exec-1"):
        with pytest.raises(TypeError, match="runtime Ref"):
            dml.runtime.read_launch_state(execution)
    assert calls == []


@pytest.mark.parametrize("operation", ["read_execution_record", "read_launch_state", "describe_graph", "cancel"])
def test_runtime_identity_methods_reject_wrong_ref_namespace(tmp_path, monkeypatch, operation) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(TypeError, match="runtime Ref"):
        getattr(dml.runtime, operation)(Ref("dag:not-runtime"))


def test_runtime_graph_and_cancel_reject_strings_before_delegation(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    state = NoopExecutionState()
    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: state)
    monkeypatch.setattr(dml_mod, "_index_ops", lambda _dml: local_index_ops(state))

    for execution in ("exec-1", "index:exec-1"):
        with pytest.raises(TypeError, match="runtime Ref"):
            dml.runtime.describe_graph(execution)
        with pytest.raises(TypeError, match="runtime Ref"):
            dml.runtime.cancel(execution)
    assert state.cancel_calls == []


def test_runtime_describe_graph_visual_renders_and_returns_none(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()
    rendered = {}

    def fake_render(graph) -> None:
        rendered["graph"] = graph

    monkeypatch.setattr(dml_mod, "_render_execution_graph", fake_render)

    result = dml.runtime.describe_graph(index, visual=True)

    assert result is None
    assert rendered["graph"]["roots"] == [index.id()]
    assert rendered["graph"]["nodes"][index.id()]["execution_id"] == index.id()


def test_runtime_describe_graph_visual_requires_terminal_extra(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "rich", None)

    with pytest.raises(DmlRepoError, match=r"daggerml\[terminal\]"):
        dml_mod._render_execution_graph({"roots": [], "nodes": {}})


def test_runtime_describe_graph_visual_renderer_shows_stack_metadata_and_edges(monkeypatch, capsys) -> None:
    fake_rich = ModuleType("rich")
    fake_rich.box = type("Box", (), {"SIMPLE_HEAD": "simple", "ROUNDED": "rounded"})

    fake_console = ModuleType("rich.console")

    class Group:
        def __init__(self, *parts) -> None:
            self.parts = parts

        def __str__(self) -> str:
            return "\n".join(str(part) for part in self.parts)

    class Console:
        def print(self, obj) -> None:
            print(obj)

    fake_console.Group = Group
    fake_console.Console = Console

    fake_panel = ModuleType("rich.panel")

    class Panel:
        def __init__(self, renderable, title=None, **kwargs) -> None:
            self.renderable = renderable
            self.title = title

        def __str__(self) -> str:
            prefix = f"{self.title}\n" if self.title else ""
            return prefix + str(self.renderable)

    fake_panel.Panel = Panel

    fake_table = ModuleType("rich.table")

    class Table:
        def __init__(self, *args, title=None, **kwargs) -> None:
            self.title = title
            self.rows = []

        @classmethod
        def grid(cls, **kwargs):
            return cls()

        def add_column(self, *args, **kwargs) -> None:
            return None

        def add_row(self, *items) -> None:
            self.rows.append(items)

        def __str__(self) -> str:
            body = "\n".join(" | ".join(str(item) for item in row) for row in self.rows)
            return f"{self.title}\n{body}" if self.title else body

    fake_table.Table = Table

    fake_text = ModuleType("rich.text")

    class Text(str):
        def __new__(cls, value="", *args, **kwargs):
            return str.__new__(cls, value)

        @classmethod
        def assemble(cls, *parts):
            return cls("".join(part[0] if isinstance(part, tuple) else str(part) for part in parts))

    fake_text.Text = Text

    fake_tree = ModuleType("rich.tree")

    class Tree:
        def __init__(self, label, **kwargs) -> None:
            self.label = label
            self.children = []

        def add(self, label):
            child = Tree(label)
            self.children.append(child)
            return child

        def __str__(self) -> str:
            lines = []

            def walk(node, depth: int) -> None:
                lines.append(f"{'  ' * depth}{node.label}")
                for child in node.children:
                    walk(child, depth + 1)

            walk(self, 0)
            return "\n".join(lines)

    fake_tree.Tree = Tree

    monkeypatch.setitem(sys.modules, "rich", fake_rich)
    monkeypatch.setitem(sys.modules, "rich.console", fake_console)
    monkeypatch.setitem(sys.modules, "rich.panel", fake_panel)
    monkeypatch.setitem(sys.modules, "rich.table", fake_table)
    monkeypatch.setitem(sys.modules, "rich.text", fake_text)
    monkeypatch.setitem(sys.modules, "rich.tree", fake_tree)
    monkeypatch.setattr(dml_mod, "time", lambda: 200)

    dml_mod._render_execution_graph(
        {
            "roots": ["root-exec"],
            "nodes": {
                "root-exec": {
                    "execution_id": "root-exec",
                    "cache_key": "cache-root",
                    "lifecycle": "running",
                    "updated_at": 190,
                    "created_at": 100,
                    "cancel_requested_by": None,
                    "children": ["child-exec"],
                    "spawned": ["spawned-exec"],
                },
                "child-exec": {
                    "execution_id": "child-exec",
                    "cache_key": "cache-child",
                    "lifecycle": "succeeded",
                    "updated_at": 175,
                    "created_at": 120,
                    "cancel_requested_by": None,
                    "children": [],
                    "spawned": [],
                },
                "spawned-exec": {
                    "execution_id": "spawned-exec",
                    "cache_key": "cache-spawned",
                    "lifecycle": "failed",
                    "updated_at": 198,
                    "created_at": 180,
                    "cancel_requested_by": "alice",
                    "children": [],
                    "spawned": [],
                },
            },
        }
    )

    out = capsys.readouterr().out

    assert "Execution Call Stack" in out
    assert "root-exec" in out
    assert "cache-root" in out
    assert "RUNNING call 1m 40s -> 10s [1 : 1]" in out
    assert "SUCCEEDED call 1m 20s -> 25s [0 : 0]" in out
    assert "FAILED call 20s -> 2s [0 : 0]" in out
    assert "child-exec" in out
    assert "spawned-exec" in out
    assert "alice" in out
    assert "Edges" not in out


def test_first_named_commit_materializes_unborn_branch_ref_and_branch_list(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    head = Head(str(tmp_path))

    commit = commit_literal_dag(dml, "train", 1, message="train-v1")

    assert head.get_local_ref("main") == commit
    assert dml.branch.list() == [{"name": "main", "commit": commit}]
