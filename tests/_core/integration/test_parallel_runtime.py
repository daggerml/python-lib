from __future__ import annotations

import daggerml._core.dml as dml_mod
from daggerml._core.dml import Dml
from daggerml._core.types import Dag
from tests._core.helpers import local_index_ops, run_parallel


def _repo(tmp_path, monkeypatch):
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    Dml.init(str(tmp_path), user="tester", remote_root="s3://bucket/root")
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: local_index_ops())
    return Dml(str(tmp_path), remote_root="s3://bucket/root", user="tester")


def test_concurrent_create_returns_distinct_readable_indexes(tmp_path, monkeypatch) -> None:
    dml = _repo(tmp_path, monkeypatch)
    indexes = run_parallel(4, lambda _: dml.runtime.create())
    assert len(set(indexes)) == 4
    with dml._db.tx(readonly=True) as txn:
        for index in indexes:
            ctx = txn.get_ctx(index)
            assert isinstance(ctx.dag, Dag)
            assert ctx.commit == txn.get(index)


def test_same_index_distinct_names_preserve_all_nodes(tmp_path, monkeypatch) -> None:
    dml = _repo(tmp_path, monkeypatch)
    index = dml.runtime.create()
    nodes = run_parallel(4, lambda i: dml.runtime.put_literal(index, i, name=f"n{i}"))
    with dml._db.tx(readonly=True) as txn:
        dag = txn.get_ctx(index).dag
        assert dag is not None
        assert dag.names == {f"n{i}": nodes[i] for i in range(4)}
        assert set(nodes).issubset(set(dag.nodes))


def test_same_index_conflicting_name_remains_coherent(tmp_path, monkeypatch) -> None:
    dml = _repo(tmp_path, monkeypatch)
    index = dml.runtime.create()
    nodes = run_parallel(4, lambda i: dml.runtime.put_literal(index, i, name="shared"))
    with dml._db.tx(readonly=True) as txn:
        dag = txn.get_ctx(index).dag
        assert dag is not None
        assert dag.names["shared"] in set(nodes)
        assert dag.names["shared"] in set(dag.nodes)


def test_reads_during_writes_observe_coherent_states(tmp_path, monkeypatch) -> None:
    dml = _repo(tmp_path, monkeypatch)

    def worker(i: int):
        if i == 0:
            for n in range(3):
                index = dml.runtime.create()
                node = dml.runtime.put_literal(index, n, name=f"n{n}")
                dml.runtime.commit(index, node, name=f"dag{n}")
            return "writer"
        snapshots = []
        for _ in range(8):
            commit = dml.status()["commit"]
            log_len = len(dml.log()["commits"]) if commit is not None else 0
            snapshots.append((commit, len(dml.runtime.list()), log_len))
        return snapshots

    results = run_parallel(4, worker)
    assert results[0] == "writer"
    for snapshots in results[1:]:
        assert snapshots
        assert all(
            (commit is None and log_len == 0)
            or (commit is not None and commit.ns() == "commit" and log_len >= 1)
            for commit, _num_indexes, log_len in snapshots
        )
