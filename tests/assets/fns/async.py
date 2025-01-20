import os
import sys

from daggerml.helper import Dml

with Dml(data=sys.stdin.read()) as dml:
    cache_dir = os.getenv('DML_FN_CACHE_DIR', '')
    cache_file = os.path.join(cache_dir, dml.cache_key)
    debug_file = os.path.join(cache_dir, 'debug')

    with open(debug_file, 'a') as f:
        f.write('ASYNC EXECUTING\n')

    if os.path.isfile(cache_file):
        with dml.new('test', 'test') as d0:
            d0.commit(42)
    else:
        open(cache_file, 'w').close()
