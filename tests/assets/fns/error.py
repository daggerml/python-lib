try:
    import os
    import sys

    from daggerml import Error, from_json, to_json
    from daggerml.helper import Dml

    dag, dump = from_json(sys.stdin.read())
    cache_key = dag.to.split('/', 1)[1]
    cache_dir = os.getenv('DML_FN_CACHE_DIR', '')
    cache_file = os.path.join(cache_dir, cache_key) if cache_dir else None
    debug_file = os.path.join(cache_dir, 'debug')

    with open(debug_file, 'a') as f:
        f.write('ASYNC EXECUTING\n')

    if os.path.isfile(cache_file):
        try:
            with Dml.init() as dml:
                with dml.new('test', 'test', dump) as d0:
                    d0.commit(1/0)
                    print(d0.dump)
        except Exception:
            print(d0.dump)
    else:
        open(cache_file, 'w').close()
except Exception as e:
    print(to_json(Error(e)))
