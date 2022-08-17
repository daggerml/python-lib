import dml
import time
from dml import Datum, Func


@dml.func
def main():
    """showing two ways to build funcs

    You can either call `from_py` yourself, or just pass in the python types

    Check out the build and build-3 values of the returned dict.

    This is true in general. You can always return either a python variable or
    a Datum (and it can be nested, etc.). In this case, you return a dict, with
    a Datum nested inside. It's fine. dml.py converts it all recursively.
    """
    executor = 'docker-build2'
    dml.createExecutor(executor)
    func_datum = Datum.from_py('build2')
    func = Func(executor, func_datum)
    return {'build': func, 'build-3': Func(executor, 'build3')}


if __name__ == '__main__':
    dml.run(main, dag_id='docker-' + str(time.time()))
