import sys

from daggerml import Dml

with Dml(data=sys.stdin.read(), message_handler=print) as dml:
    with dml.new('test', 'test') as d0:
        d0.commit(sum(d0.expr[1:].value()))
