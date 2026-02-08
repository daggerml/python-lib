import json
import sys
import tempfile
from uuid import uuid4

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES, Commit, Head, Tree


def _init_repo(db: DmlDbEnv) -> None:
    with IndexOps(db)._tx(readonly=False) as txn:
        tree_ref = txn.put(Tree(dags={}))
        commit_ref = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="initial"))
        txn.put(Head(commit=commit_ref), to=DEFAULT_HEAD)


if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    remote_root = remote["root"]
    remote_cache = remote["cache"]
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            _init_repo(db)
            ops = IndexOps(db, remote_root=remote_root, remote_cache=remote_cache)
            index_ref = ops.create(argv_ptr=argv_ptr)
            result = ops.put_literal(index_ref, str(uuid4()))
            commit_ref = ops.commit(index_ref, result, message="rand function result")
            dag_ref = CommitOps(_db=db).describe(commit_ref)["dag"]
            CacheOps(_db=db, remote_root=remote_root, remote_cache=remote_cache).put(dag_ref)
            print(json.dumps({"status": "succeeded", "error": None}, separators=(",", ":")))
        finally:
            db.close()
