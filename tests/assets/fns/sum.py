import json
import sys

from daggerml import Dml

with Dml() as dml:
    with dml.new("test", "test", json.loads(sys.stdin.read())["dump"], print) as d0:
        d0.num_args = len(d0.argv[1:])
        d0.n0 = sum(d0.argv[1:].value())
        d0.result = d0.n0
