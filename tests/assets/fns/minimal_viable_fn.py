import sys

from daggerml import Dml

with Dml(data=sys.stdin.read(), message_handler=print) as dml:
    with dml.new('test', 'test') as d0:
        d0.result = sum(d0.argv[1:].value())
