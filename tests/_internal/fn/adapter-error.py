import json
import sys
import tempfile

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES, Commit, Error, Head, Tree


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
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            _init_repo(db)
            ops = IndexOps(db, remote_root=remote_root)
            index_ref = ops.create(argv_ptr=argv_ptr)
            try:
                raise ValueError("test error")
            except Exception as e:
                result = Error.from_ex(e)
            commit_ref = ops.commit(index_ref, result, message="adapter_error function result")
            with ops._tx(readonly=True) as txn:
                commit_obj = txn.get(commit_ref)
            dag_id = commit_obj.dag.id()
            print(json.dumps({"status": "succeeded", "error": None, "dag_id": dag_id}, separators=(",", ":")))
        finally:
            db.close()
