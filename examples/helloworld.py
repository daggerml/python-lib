#!/usr/bin/env python
import daggerml as dml

if __name__ == "__main__":
    dag = dml.Dag.new("Oren's Dag","hello world")
    n1 = dag.put({"Foo": (1,2,"bar",True)})
    n2 = dag.put({"Foo": [1,2,"bar",True]})
    n3 = dag.put({"Foo": (1,2,"bar",False)})
    n4 = dag.put([n1, n2, n3])
    dag.commit(n3)
    print(f'{n1 = }')
    print(f'{n2 = }')
    print(f'{n3 = }')
    print(f'{n4 = }')
    print(f'{dag.get_value(n1) = }')
    print(f'{dag.get_value(n2) = }')
    print(f'{dag.get_value(n3) = }')
    print(f'{dag.get_value(n4) = }')
