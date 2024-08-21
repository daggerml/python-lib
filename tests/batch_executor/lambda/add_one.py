import sys
from numbers import Number

import daggerml as dml

if __name__ == '__main__':
    with dml.Api(initialize=True) as api:
        dag = dml.new('execution', 'foo', dump=sys.stdin.read(), api=api)
        _, *expr = dag.expr
        result = []
        for x in expr:
            assert isinstance(x, Number)
            result.append(x + 1)
        print(api.dump(dag.commit(result)))
