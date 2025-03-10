import json
import os
import sys

from daggerml import Dml

# print(sys.stdin.read(), file=sys.stderr)
with Dml() as dml:
    stdin = json.loads(sys.stdin.read())
    cache_dir = os.getenv("DML_FN_CACHE_DIR", "")
    cache_file = os.path.join(cache_dir, stdin["cache_key"])
    debug_file = os.path.join(cache_dir, "debug")

    with open(debug_file, "a") as f:
        f.write("ASYNC EXECUTING\n")

    if os.path.isfile(cache_file):
        with dml.new("test", "test", stdin["dump"], print) as d0:
            d0.result = sum(d0.argv[1:].value())
    else:
        open(cache_file, "w").close()
