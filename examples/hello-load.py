#!/usr/bin/env python
import daggerml as dml
from daggerml.executor import Sh

try:
    import nevergrad as ng
except ImportError as e:
    print('rahhh', e)



def _fn(x: dict):
    import nevergrad as ng
    from numbers import Number
    def inner_fn(lst):
        return [z ** 2 if isinstance(z, Number) and not isinstance(z, bool) else z
                for z in lst]
    print(f'{x = }')
    return [str(ng), {k: inner_fn(v) for k, v in x.items()}]

if __name__ == "__main__":
    dag = dml.Dag.new("Oren's Load Dag","hello load")
    node = dag.load("Oren's Dag")
    n3 = Sh(dag).run_hatch(_fn, node, env='test-nevergrad')
    print(f'{n3 = }')
    print(f'{dag.get_value(n3) = }')
    dag.commit(n3)
