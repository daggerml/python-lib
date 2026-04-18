import json
import sys
import tempfile
from uuid import uuid4

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import NAMESPACES

if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    remote_root = remote["root"]
    runnable_kwargs = envelope.get("runnable", {}).get("kwargs", {})
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            ops = IndexOps(db, remote_root=remote_root)
            index_ref = ops.create(argv_ptr=argv_ptr)
            node_ops = NodeOps(db)
            argv = node_ops.unroll(ops.get_argv(index_ref))
            for key, value in runnable_kwargs.items():
                ops.put_literal(index_ref, value, name=key)
            ops.put_literal(index_ref, len(argv[1:]), name="num_args")
            result = ops.put_literal(index_ref, sum(argv[1:]), name="n0")
            ops.put_literal(index_ref, str(uuid4()), name="uuid")
            ops.commit(index_ref, result, message="sum")
            print(json.dumps({"status": "succeeded", "error": None}, separators=(",", ":")))
        finally:
            db.close()
