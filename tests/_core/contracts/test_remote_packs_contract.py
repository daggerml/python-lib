"""Graph packs are complete immutable units, not loose CAS-object uploads."""

import gzip
import json
from collections import Counter
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace

import pytest

from daggerml._core.remote import Remote
from daggerml._core.s3_cas import CasItemConflict
from daggerml._core.types import (
    ArgvNode,
    Commit,
    Dag,
    DmlDB,
    DmlRepoError,
    FnNode,
    ImportNode,
    ListDatum,
    LiteralNode,
    ScalarDatum,
    Tree,
)


def database(path):
    path.mkdir()
    db = DmlDB(str(path), 1024 * 1024, 64 * 1024 * 1024)
    with db.tx(create_if_missing=True):
        pass
    return db


@pytest.fixture
def graph(tmp_path):
    source, target = database(tmp_path / "source"), database(tmp_path / "target")
    data, writes = {}, []

    def put(key, payload, *, overwrite=True):
        if key in data and not overwrite:
            raise CasItemConflict(key)
        data[key] = payload
        writes.append(key)

    remote = object.__new__(Remote)
    remote.n_workers = 2
    remote._store = SimpleNamespace(
        _key_for=lambda key: key, _exists=lambda key: key in data, _put=put,
        _get=lambda key: data[key], _get_bytes=lambda key: data[key],
        _put_js=lambda key, value, **kwargs: put(key, json.dumps(value), **kwargs),
    )
    with source.tx() as txn:
        value = txn.put(ScalarDatum("shared"))
        argv = txn.put(ArgvNode(value=txn.put(ListDatum([value]))))
        node = txn.put(LiteralNode(value=value))
        nested = txn.put(Dag(nodes=[node], names={}, tags=[], result=node))
        imported = txn.put(ImportNode(dag=nested, node=node))
        dag = txn.put(Dag(nodes=[argv, imported], names={"result": imported}, tags=[], result=imported, argv=argv))
        tree = txn.put(Tree(dags={"main": dag, "alias": dag}))
        parent = txn.put(Commit(parents=[], tree=tree, author="a", message="parent"))
        commit = txn.put(Commit(parents=[parent], tree=tree, author="a", message="tip"))
    return SimpleNamespace(remote=remote, source=source, target=target, data=data, writes=writes,
                           argv=argv, dag=dag, nested=nested, value=value, node=node,
                           commit=commit, parent=parent, tree=tree)


def test_project_upload_only_writes_manifests_and_deduplicated_dag_packs(graph):
    g = graph
    g.remote.upload_object_graph(g.commit, g.source)
    # The tree names the same DAG twice; each graph body is uploaded once.
    assert g.writes.count(g.remote._pack_key(g.dag)) == 1
    assert g.writes.count(g.remote._pack_key(g.nested)) == 1
    g.remote.materialize_ref(g.commit, g.target)
    with g.target.tx(readonly=True) as txn:
        assert all(txn.exists(ref) for ref in (g.commit, g.parent, g.tree, g.dag, g.nested, g.argv, g.value))
    before = list(g.writes)
    g.remote.upload_object_graph(g.commit, g.source)
    assert g.writes == before


@pytest.mark.parametrize("root_name", ["dag", "argv"])
def test_same_codec_roundtrips_dag_and_argv_without_synthetic_dag(graph, root_name):
    g = graph
    root = getattr(g, root_name)
    g.remote.upload_object_graph(root, g.source)
    raw = g.data[g.remote._pack_key(root)]
    envelope = json.loads(gzip.decompress(raw))
    assert envelope["root"] == root.to
    if root_name == "argv":
        assert not any(key.startswith("dag:") for key in envelope["objects"])
    assert g.remote.materialize_ref(root, g.target) == root
    with g.source.tx(readonly=True) as source, g.target.tx(readonly=True) as target:
        assert target.get(root) == source.get(root)
        assert target.get(g.value) == source.get(g.value)


@pytest.mark.parametrize("damage", ["schema", "root", "missing", "unreachable", "identity", "gzip"])
def test_damaged_pack_never_materializes_root(graph, damage):
    g = graph
    g.remote.upload_object_graph(g.dag, g.source)
    key = g.remote._pack_key(g.dag)
    envelope = json.loads(gzip.decompress(g.data[key]))
    if damage == "schema":
        envelope["schema"] = True
    elif damage == "root":
        envelope["root"] = g.argv.to
    elif damage == "missing":
        del envelope["objects"][g.value.to]
    elif damage == "unreachable":
        envelope["objects"][g.commit.to] = "{}"
    elif damage == "identity":
        envelope["objects"][g.value.to] = g.remote._dump_cas_object(ScalarDatum("different"))
    raw = json.dumps(envelope)
    g.data[key] = b"not gzip" if damage == "gzip" else gzip.compress(raw.encode())
    with pytest.raises((ValueError, gzip.BadGzipFile)):
        g.remote.materialize_ref(g.dag, g.target)
    with g.target.tx(readonly=True) as txn:
        assert not txn.exists(g.dag)
        assert not txn.exists(g.value)


def test_pack_identity_is_checked_even_when_a_contained_object_already_exists(graph):
    g = graph
    with g.target.tx() as txn:
        txn.put(ScalarDatum("shared"))
    g.remote.upload_object_graph(g.dag, g.source)
    key = g.remote._pack_key(g.dag)
    envelope = json.loads(gzip.decompress(g.data[key]))
    envelope["objects"][g.value.to] = g.remote._dump_cas_object(ScalarDatum("different"))
    g.data[key] = gzip.compress(json.dumps(envelope).encode())
    with pytest.raises(ValueError, match="identity mismatch"):
        g.remote.materialize_ref(g.dag, g.target)


def test_shallow_fetch_does_not_read_ancestor_manifest(graph):
    g = graph
    g.remote.upload_object_graph(g.commit, g.source)
    del g.data[g.remote._commit_key(g.parent)]
    root, available, omitted = g.remote.materialize_project_commit(
        g.remote._build_ref_payload(g.commit), g.target, depth=1,
    )
    assert root == g.commit
    assert available == {g.commit}
    assert omitted == {g.parent}
    with g.target.tx(readonly=True) as txn:
        assert txn.exists(g.nested)
        assert not txn.exists(g.parent)


def test_conflicting_immutable_upload_does_not_overwrite_data(graph):
    graph.data["key"] = b"old"
    with pytest.raises(DmlRepoError, match="Conflicting immutable"):
        graph.remote._put_immutable("key", b"new")
    assert graph.data["key"] == b"old"


def test_equivalent_pack_with_different_compressed_bytes_is_not_a_conflict(graph):
    g = graph
    key = g.remote._pack_key(g.dag)
    content = b'{"same":"graph"}'
    g.data[key] = gzip.compress(content, compresslevel=1, mtime=0)
    stored = g.data[key]
    g.remote._put_immutable(key, gzip.compress(content, compresslevel=9, mtime=5))
    assert g.data[key] == stored
    with pytest.raises(DmlRepoError, match="Conflicting immutable"):
        g.remote._put_immutable(key, gzip.compress(b'{"other":"graph"}'))
    assert g.data[key] == stored


def function_graph(g):
    with g.source.tx() as txn:
        inner = txn.put(FnNode(argv=[], dag=g.nested))
        middle = txn.put(Dag(nodes=[inner], names={}, tags=["middle"], result=inner))
        call = txn.put(FnNode(argv=[], dag=middle))
        imported = txn.put(ImportNode(dag=g.dag, node=g.argv))
        outer = txn.put(Dag(nodes=[call, imported], names={}, tags=["outer"], result=call))
    return outer, middle


def test_bounded_bodies_flatten_functions_and_resolve_imports_before_download(graph):
    g = graph
    root, middle = function_graph(g)
    g.remote.upload_object_graph(root, g.source)
    manifest = json.loads(g.data[g.remote._manifest_key(root)])
    assert manifest["subdags"] == sorted([middle.to, g.nested.to])
    assert manifest["imports"] == [g.dag.to]
    body = json.loads(gzip.decompress(g.data[g.remote._pack_key(root)]))["objects"]
    assert not {middle.to, g.nested.to, g.dag.to, g.argv.to, g.node.to} & body.keys()
    reads = []

    def get(key):
        reads.append(key)
        return g.data[key]

    g.remote._store._get_bytes = get
    g.remote.materialize_ref(root, g.target)
    manifest_reads = [i for i, key in enumerate(reads) if key.startswith("manifests/")]
    body_reads = [i for i, key in enumerate(reads) if key.startswith("packs/")]
    assert max(manifest_reads) < min(body_reads)
    assert len(body_reads) == len(set(reads[i] for i in body_reads))
    with g.target.tx(readonly=True) as txn:
        assert all(txn.exists(ref) for ref in (root, middle, g.nested, g.dag, g.argv, g.node))


@pytest.mark.parametrize("damage", ["omitted", "extra", "selected-node"])
def test_manifest_or_cross_pack_damage_cannot_publish_root(graph, damage):
    g = graph
    outer, middle = function_graph(g)
    g.remote.upload_object_graph(outer, g.source)
    key = g.remote._manifest_key(outer)
    manifest = json.loads(g.data[key])
    if damage == "omitted":
        manifest["subdags"].remove(middle.to)
    elif damage == "extra":
        manifest["subdags"] = sorted([*manifest["subdags"], g.dag.to])
    else:
        # A selected import node must exist even though its edge is not packed.
        key = g.remote._pack_key(g.dag)
        envelope = json.loads(gzip.decompress(g.data[key]))
        del envelope["objects"][g.argv.to]
        g.data[key] = gzip.compress(json.dumps(envelope).encode())
    if damage != "selected-node":
        g.data[key] = json.dumps(manifest)
    with pytest.raises(ValueError):
        g.remote.materialize_ref(outer, g.target)
    with g.target.tx(readonly=True) as txn:
        assert not txn.exists(outer)


def test_incomplete_function_objects_from_failed_fetch_are_not_trusted_on_retry(graph):
    g = graph
    outer, middle = function_graph(g)
    g.remote.upload_object_graph(outer, g.source)
    # Simulate a function batch committed before a later network failure.
    objects = g.remote._decode_pack(middle, g.data[g.remote._pack_key(middle)])
    g.remote._materialize_objects(list(objects.items()), g.target)
    with g.target.tx(readonly=True) as txn:
        assert txn.exists(middle)
        assert not txn.exists(g.nested)
    # Execution results may select that same function DAG as a new root.
    g.remote.upload_object_graph(middle, g.source)
    g.remote.materialize_ref(middle, g.target)
    with g.target.tx(readonly=True) as txn:
        assert txn.exists(g.nested)
    g.remote.materialize_ref(outer, g.target)


def test_function_insertion_overlaps_download_and_stays_on_caller(graph):
    from tests._core.contracts.test_remote_fetch_concurrency_contract import CallerOnlyDB

    g = graph
    with g.source.tx() as txn:
        children = [txn.put(Dag(nodes=[g.node], names={}, tags=[str(i)], result=g.node)) for i in range(2)]
        calls = [txn.put(FnNode(argv=[], dag=child)) for child in children]
        root = txn.put(Dag(nodes=calls, names={}, tags=[], result=calls[0]))
    g.remote.upload_object_graph(root, g.source)
    slow, fast = sorted(children)
    inserted = Event()
    target = CallerOnlyDB(g.target)
    original_write = target.write_with_growth

    def write(callback):
        result = original_write(callback)
        with target.tx(readonly=True) as txn:
            if txn.exists(fast):
                inserted.set()
        return result

    target.write_with_growth = write

    def get(key):
        if key == g.remote._pack_key(slow):
            assert inserted.wait(timeout=5), "Download waited for a batch instead of streaming insertion"
        return g.data[key]

    g.remote._store._get_bytes = get
    g.remote.materialize_ref(root, target)
    assert inserted.is_set()
    assert target.writes >= 2


@pytest.mark.parametrize("root_name", ["dag", "argv"])
def test_published_graph_upload_stops_without_opening_local_database(graph, root_name):
    g = graph
    root = getattr(g, root_name)
    g.remote.upload_object_graph(root, g.source)
    before = list(g.writes)
    checks = []

    def exists(key):
        checks.append(key)
        return key in g.data

    def unexpected(*args, **kwargs):
        pytest.fail("Published graph uploads must not inspect local or remote bodies/dependencies")

    g.remote._store._exists = exists
    g.remote._store._get_bytes = unexpected
    g.remote.upload_object_graph(root, SimpleNamespace(tx=unexpected))
    assert checks == [g.remote._manifest_key(root)]
    assert g.writes == before


@pytest.mark.parametrize("indirect_import", [False, True])
def test_new_graph_stops_at_published_functions_and_imports_and_partitions_new_body_once(
    graph, monkeypatch, indirect_import,
):
    g = graph
    root, middle = function_graph(g)
    if indirect_import:
        with g.source.tx() as txn:
            imported = txn.put(ImportNode(dag=g.dag, node=g.argv))
            inner = txn.put(FnNode(argv=[], dag=g.nested))
            middle = txn.put(Dag(nodes=[inner, imported], names={}, tags=[], result=imported))
            call = txn.put(FnNode(argv=[], dag=middle))
            root = txn.put(Dag(nodes=[call], names={}, tags=[], result=call))
    g.remote.upload_object_graph(g.dag, g.source)
    g.remote.upload_object_graph(middle, g.source)
    partitions, reads = [], []
    partition = g.remote._partition

    def local_body(ref, get):
        partitions.append(ref)
        assert ref == root, "Do not walk already-published graphs or their descendants"
        return partition(ref, get)

    def get(key):
        reads.append(key)
        return g.data[key]

    monkeypatch.setattr(g.remote, "_partition", local_body)
    g.remote._store._get_bytes = get
    g.remote.upload_object_graph(root, g.source)
    assert partitions == [root]
    assert reads == [g.remote._manifest_key(middle)]
    manifest = json.loads(g.data[g.remote._manifest_key(root)])
    assert set(manifest["subdags"]) == {middle.to, g.nested.to}
    assert manifest["imports"] == [g.dag.to]
    monkeypatch.setattr(g.remote, "_partition", partition)
    g.remote.materialize_ref(root, g.target)
    with g.target.tx(readonly=True) as txn:
        assert all(txn.exists(ref) for ref in (root, middle, g.nested, g.dag, g.argv, g.node))


def test_interrupted_import_upload_does_not_publish_user_manifest_and_can_resume(graph):
    g = graph
    put = g.remote._store._put

    def fail_import_manifest(key, payload, **kwargs):
        if key == g.remote._manifest_key(g.nested):
            raise OSError("interrupted upload")
        put(key, payload, **kwargs)

    g.remote._store._put = fail_import_manifest
    with pytest.raises(OSError, match="interrupted upload"):
        g.remote.upload_object_graph(g.dag, g.source)
    assert g.remote._manifest_key(g.dag) not in g.data
    assert g.remote._pack_key(g.dag) in g.data
    assert g.remote._pack_key(g.nested) in g.data
    g.remote._store._put = put
    g.remote.upload_object_graph(g.dag, g.source)
    assert g.writes.index(g.remote._manifest_key(g.nested)) < g.writes.index(g.remote._manifest_key(g.dag))
    assert g.writes.count(g.remote._pack_key(g.dag)) == 1
    assert g.writes.count(g.remote._pack_key(g.nested)) == 1
    g.remote.materialize_ref(g.dag, g.target)


def test_published_commit_upload_stops_without_opening_local_database(graph):
    g = graph
    g.remote.upload_object_graph(g.commit, g.source)
    before = list(g.writes)
    checks = []

    def exists(key):
        checks.append(key)
        return key in g.data

    def unexpected(*args, **kwargs):
        pytest.fail("Published commits must stop history and graph traversal")

    g.remote._store._exists = exists
    g.remote._store._get_bytes = unexpected
    g.remote.upload_object_graph(g.commit, SimpleNamespace(tx=unexpected))
    assert checks == [g.remote._commit_key(g.commit)]
    assert g.writes == before


@pytest.mark.parametrize("workers", [1, 2])
def test_incremental_merge_upload_stops_at_shared_remote_parent(graph, workers):
    g = graph
    g.remote.n_workers = workers
    g.remote.upload_object_graph(g.parent, g.source)
    with g.source.tx() as txn:
        left, right = [txn.put(Commit(parents=[g.parent], tree=g.tree, author="a", message=message))
                       for message in ("left", "right")]
        merge = txn.put(Commit(parents=[left, right], tree=g.tree, author="a", message="merge"))
    reads, checks = [], []

    @contextmanager
    def local_tx(**kwargs):
        with g.source.tx(**kwargs) as txn:
            def get(ref):
                reads.append(ref)
                assert ref in {merge, left, right, g.tree}, "Do not read published history or graph bodies"
                return txn.get(ref)
            yield SimpleNamespace(get=get, exists=txn.exists)

    def exists(key):
        checks.append(key)
        return key in g.data

    g.remote._store._exists = exists
    g.remote.upload_object_graph(merge, SimpleNamespace(tx=local_tx))
    assert Counter(reads) == Counter({merge: 1, left: 1, right: 1, g.tree: 3})
    assert Counter(checks) == Counter(dict.fromkeys(
        [g.remote._commit_key(ref) for ref in (merge, left, right, g.parent)] + [g.remote._manifest_key(g.dag)], 1,
    ))
    assert g.writes.index(g.remote._commit_key(left)) < g.writes.index(g.remote._commit_key(merge))
    assert g.writes.index(g.remote._commit_key(right)) < g.writes.index(g.remote._commit_key(merge))
    g.remote.materialize_ref(merge, g.target)
    with g.target.tx(readonly=True) as txn:
        assert all(txn.exists(ref) for ref in (merge, left, right, g.parent, g.tree, g.dag, g.nested))


def test_interrupted_parent_commit_upload_does_not_publish_child_or_ref_and_can_resume(graph):
    g = graph
    put = g.remote._store._put

    def fail_parent(key, payload, **kwargs):
        if key == g.remote._commit_key(g.parent):
            raise OSError("interrupted parent upload")
        put(key, payload, **kwargs)

    g.remote._store._put = fail_parent
    with pytest.raises(OSError, match="interrupted parent upload"):
        g.remote._put_cas(g.commit, "refs/heads/main.json", g.source)
    assert g.remote._commit_key(g.commit) not in g.data
    assert "refs/heads/main.json" not in g.data
    g.remote._store._put = put
    g.remote._put_cas(g.commit, "refs/heads/main.json", g.source)
    assert g.writes.index(g.remote._commit_key(g.parent)) < g.writes.index(g.remote._commit_key(g.commit))
    assert g.writes.index(g.remote._commit_key(g.commit)) < g.writes.index("refs/heads/main.json")
    g.remote.materialize_ref(g.commit, g.target)
