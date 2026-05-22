import json
import sys
import tempfile
from pathlib import Path
from typing import cast

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import NAMESPACES, Error

if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    execution_id = envelope["execution_id"]
    remote_root = remote["root"]
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db_path = Path(tmpdir) / ".dml" / "db"
        db_path.mkdir(parents=True, exist_ok=True)
        db = DmlDbEnv.create(str(db_path), namespaces=sorted(NAMESPACES))
        try:
            ops = IndexOps(db, remote_root=remote_root)
            index_ref = ops.create(argv_ptr=argv_ptr)
            node_ops = NodeOps(db)
            argv = cast(list, node_ops.unroll(ops.get_argv(index_ref)))
            _, *args = argv
            try:
                for i, arg in enumerate(args):
                    if not isinstance(arg, (int, float)):
                        raise TypeError(f"Argument at index {i} is {type(arg).__name__}, expected int or float")
                result = ops.put_literal(index_ref, float(sum(args)))
            except Exception as e:
                result = Error.from_ex(e)
            commit_ref = ops.commit(index_ref, result, message="sum function result", execution_id=execution_id)
            with ops._tx(readonly=True) as txn:
                commit_obj = txn.get(commit_ref)
            dag_id = commit_obj.dag.id()
            print(json.dumps({"status": "succeeded", "error": None, "dag_id": dag_id}, separators=(",", ":")))
        finally:
            db.close()
