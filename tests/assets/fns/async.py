import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.execution_context import execution_context
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import NAMESPACES, Error

if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    execution_id = envelope["execution_id"]
    execution_cache_key = envelope["cache_key"]
    remote_root = remote["root"]
    cache_key = hashlib.sha256(argv_ptr.encode()).hexdigest()
    cache_dir = os.getenv("DML_TEST_FN_STATE_DIR", "")
    cache_file = os.path.join(cache_dir, cache_key)
    debug_file = os.path.join(cache_dir, "debug")

    with open(debug_file, "a", encoding="utf-8") as fh:
        fh.write("ASYNC EXECUTING\n")

    if not os.path.isfile(cache_file):
        open(cache_file, "w", encoding="utf-8").close()
        print(
            json.dumps(
                {"status": "running", "error": None, "state": {"cache_file": cache_file}},
                separators=(",", ":"),
            )
        )
        raise SystemExit(0)

    with execution_context(execution_id, execution_cache_key):
        with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
            db_path = Path(tmpdir) / ".dml" / "db"
            db_path.mkdir(parents=True, exist_ok=True)
            db = DmlDbEnv.create(str(db_path), namespaces=sorted(NAMESPACES))
            try:
                ops = IndexOps(db, remote_root=remote_root)
                index_ref = ops.create(argv_ptr=argv_ptr)
                node_ops = NodeOps(db)
                argv = node_ops.unroll(ops.get_argv(index_ref))
                try:
                    result = ops.put_literal(index_ref, sum(argv[1:]))
                except Exception as e:
                    result = Error.from_ex(e)
                commit_ref = ops.commit(index_ref, result, message="async")
                with ops._tx(readonly=True) as txn:
                    commit_obj = txn.get(commit_ref)
                dag_id = commit_obj.dag.id()
                print(json.dumps({"status": "succeeded", "error": None, "dag_id": dag_id}, separators=(",", ":")))
            finally:
                db.close()
