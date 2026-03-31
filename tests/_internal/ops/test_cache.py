import os
from uuid import uuid4

import pytest

from daggerml._internal._db import Ref
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.types import ArgvNode, Dag, DictDatum, DmlRepoError, KwargvNode, ListDatum, ScalarDatum


def _remote_root_from_env() -> str:
    return os.environ["DML_REMOTE_ROOT"]


def _put_datum_hashed(temp_bo, data) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        return txn.put(ScalarDatum(data=data))


def _put_argv_node_hashed(temp_bo, datum_ref: Ref) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        argv_datum_ref = txn.put(ListDatum(data=[datum_ref]))
        argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
        kwargv_datum_ref = txn.put(DictDatum(data={}))
        txn.put(KwargvNode(value=kwargv_datum_ref))
        return argv_node_ref


def _cache_key_for_argv(temp_bo, argv_ref: Ref) -> str:
    with temp_bo._tx(readonly=True) as txn:
        return txn.get(argv_ref).datum_ref(txn).id()


def _put_dag_hashed(temp_bo, argv_ref: Ref | None = None) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        dag = Dag(nodes=[argv_ref] if argv_ref else [], names={}, result=None, argv=argv_ref)
        return txn.put(dag)


def _new_ops(temp_bo, cache_name: str) -> CacheOps:
    return CacheOps(temp_bo._db, remote_root=_remote_root_from_env(), remote_cache=cache_name)


def test_put_get_delete_roundtrip_remote(temp_bo, s3):
    ops = _new_ops(temp_bo, f"cachetest-{uuid4().hex}")
    datum_ref = _put_datum_hashed(temp_bo, "value")
    argv_ref = _put_argv_node_hashed(temp_bo, datum_ref)
    dag_ref = _put_dag_hashed(temp_bo, argv_ref)
    cache_key = _cache_key_for_argv(temp_bo, argv_ref)

    assert ops.get(argv_ref) is None
    assert ops.put(dag_ref) == cache_key
    remote_ops, cache_name = ops._require_remote_context()
    cache_ref_obj = remote_ops._decode_ref(remote_ops._remote_get_ref(f"cache/{cache_name}/{cache_key}.json"))
    assert cache_ref_obj["targets"] == {"dag": []}
    assert ops.get(argv_ref) == dag_ref
    assert ops.delete(argv_ref) is True
    assert ops.get(argv_ref) is None
    assert ops.delete(argv_ref) is False


def test_list_limit_and_clear_remote(temp_bo, s3):
    ops = _new_ops(temp_bo, f"cachetest-{uuid4().hex}")
    entries: list[tuple[str, Ref]] = []
    for i in range(3):
        datum_ref = _put_datum_hashed(temp_bo, f"value-{i}")
        argv_ref = _put_argv_node_hashed(temp_bo, datum_ref)
        dag_ref = _put_dag_hashed(temp_bo, argv_ref)
        cache_key = _cache_key_for_argv(temp_bo, argv_ref)
        ops.put(dag_ref)
        entries.append((cache_key, dag_ref))

    limited = list(ops.list(limit=1))
    assert len(limited) == 1
    assert limited[0] in entries

    listed = list(ops.list())
    assert set(listed) == set(entries)
    assert ops.clear() == 3
    assert list(ops.list()) == []
    assert ops.clear() == 0


def test_requires_remote_context(temp_bo):
    ops = CacheOps(temp_bo._db)
    datum_ref = _put_datum_hashed(temp_bo, "value")
    argv_ref = _put_argv_node_hashed(temp_bo, datum_ref)
    dag_ref = _put_dag_hashed(temp_bo, argv_ref)

    with pytest.raises(DmlRepoError, match="Remote cache context required"):
        ops.get(argv_ref)
    with pytest.raises(DmlRepoError, match="Remote cache context required"):
        ops.put(dag_ref)
    with pytest.raises(DmlRepoError, match="Remote cache context required"):
        list(ops.list())
    with pytest.raises(DmlRepoError, match="Remote cache context required"):
        ops.delete(argv_ref)
    with pytest.raises(DmlRepoError, match="Remote cache context required"):
        ops.clear()
