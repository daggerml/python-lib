import json
import sys
import tempfile

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import NAMESPACES

if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    remote_root = remote["root"]
    remote_cache = remote["cache"]
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            ops = IndexOps(db, remote_root=remote_root, remote_cache=remote_cache)
            index_ref = ops.create(argv_ptr=argv_ptr)
            node_ops = NodeOps(db)
            argv: list[float] = node_ops.unroll(ops.get_argv(index_ref))
            kwargv: dict = node_ops.unroll(ops.get_kwargv(index_ref))
            result = ops.put_literal(index_ref, float(sum(argv[1:]) * kwargv["x"]))
            ops.commit(index_ref, result, message="prepop function result")
            print(json.dumps({"status": "succeeded", "error": None}, separators=(",", ":")))
        finally:
            db.close()
