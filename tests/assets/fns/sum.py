import json
import sys
from uuid import uuid4

from daggerml import Dml

if __name__ == "__main__":
    stdin = json.loads(sys.stdin.read())
    with Dml.temporary(cache_path=stdin["cache_path"]) as dml:
        with dml.new("test", "test", stdin["dump"], print) as d0:
            d0.num_args = len(d0.argv[1:])
            d0.n0 = sum(d0.argv[1:].value())
            d0.uuid = str(uuid4())
            d0.result = d0.n0
