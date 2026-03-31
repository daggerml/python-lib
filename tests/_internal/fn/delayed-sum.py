import json
import os
import sys
import tempfile
from pathlib import Path
from typing import cast

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
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
    remote_cache = remote["cache"]
    tmp_dir = os.environ.get("DML_TMP_DIR")
    if not tmp_dir:
        raise ValueError("DML_TMP_DIR environment variable not set")
    completion_file = Path(tmp_dir) / "completion.flag"
    if not completion_file.exists():
        completion_file.touch()
        print(json.dumps({"status": "pending", "error": None}, separators=(",", ":")))
        raise SystemExit(0)

    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmprepo:
        db = DmlDbEnv.create(tmprepo, namespaces=sorted(NAMESPACES))
        try:
            _init_repo(db)
            ops = IndexOps(db, remote_root=remote_root, remote_cache=remote_cache)
            index_ref = ops.create(argv_ptr=argv_ptr)
            node_ops = NodeOps(db)

            argv = cast(list, node_ops.unroll(ops.get_argv(index_ref)))
            _, *args = argv

            try:
                for i, arg in enumerate(args):
                    if not isinstance(arg, (int, float)):
                        raise TypeError(f"Argument at index {i} is {type(arg).__name__}, expected int or float")
                result = ops.put_literal(index_ref, float(sum(cast(list[float], args))))
            except Exception as e:
                result = Error.from_ex(e)

            ops.commit(index_ref, result, message="delayed sum function result")
            print(json.dumps({"status": "succeeded", "error": None}, separators=(",", ":")))
        finally:
            db.close()
    completion_file.unlink()
