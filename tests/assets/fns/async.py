import hashlib
import json
import os
import sys
import tempfile

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import NAMESPACES, Error

if __name__ == "__main__":
    envelope = json.loads(sys.stdin.read())
    remote = envelope["remote"]
    argv_ptr = envelope["argv_ptr"]
    remote_root = remote["root"]
    remote_cache = remote["cache"]
    cache_key = hashlib.sha256(argv_ptr.encode()).hexdigest()
    cache_dir = os.getenv("DML_FN_CACHE_DIR", "")
    cache_file = os.path.join(cache_dir, cache_key)
    debug_file = os.path.join(cache_dir, "debug")

    with open(debug_file, "a", encoding="utf-8") as fh:
        fh.write("ASYNC EXECUTING\n")

    if not os.path.isfile(cache_file):
        open(cache_file, "w", encoding="utf-8").close()
        print(json.dumps({"status": "running", "error": None}, separators=(",", ":")))
        raise SystemExit(0)

    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            ops = IndexOps(db, remote_root=remote_root, remote_cache=remote_cache)
            index_ref = ops.create(argv_ptr=argv_ptr)
            node_ops = NodeOps(db)
            argv = node_ops.unroll(ops.get_argv(index_ref))
            try:
                result = ops.put_literal(index_ref, sum(argv[1:]))
            except Exception as e:
                result = Error.from_ex(e)
            commit_ref = ops.commit(index_ref, result, message="async")
            dag_ref = CommitOps(_db=db).describe(commit_ref)["dag"]
            CacheOps(_db=db, remote_root=remote_root, remote_cache=remote_cache).put(dag_ref)
            print(json.dumps({"status": "succeeded", "error": None}, separators=(",", ":")))
        finally:
            db.close()
