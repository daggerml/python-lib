import json
import sys

from daggerml import Dml


def pr(dump):
    print(json.dumps({"dump": dump}))


with Dml() as dml:
    with dml.new("test", "test", json.loads(sys.stdin.read())["dump"], pr) as d0:
        d0.result = dml.kwargs
