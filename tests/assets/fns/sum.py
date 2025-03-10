import json
import sys

from daggerml import Dml

with Dml(data=json.loads(sys.stdin.read())["dump"], message_handler=print) as dml:
    with dml.new("test", "test") as d0:
        d0.num_args = len(d0.argv[1:])
        d0.n0 = sum(d0.argv[1:].value())
        d0.result = d0.n0
