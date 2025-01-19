try:
    import sys

    from daggerml import Error, from_json, to_json
    from daggerml.helper import Dml

    dag, dump = from_json(sys.stdin.read())

    with Dml.init() as dml:
        with dml.new('test', 'test', dump) as d0:
            # _, timeout = d0.expr.value()
            # raise Exception('asdf qwer zxcv')
            n0 = d0.put(42)
            n1 = d0.commit(n0)
        # print(d0.dump(dag))
        res = d0.dump(d0.result)
        print(res)
except Exception as e:
    print(to_json(Error(e)))
