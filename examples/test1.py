import os
import daggerml as dml
from time import sleep


here = os.path.dirname(os.path.realpath(__file__))


@dml.func
def doit(n):
    print('running doit with', dml.to_py(n))
    datum = {'sdf': [1, 2, 3, n, ['four', 'five']]}
    sleep(1)
    return datum


@dml.func
def doit2(x):
    return x['sdf']


@dml.func
def aaron0(x):
    return x


@dml.func
def main():
    docker = dml.load('docker')
    tarball = dml.tar(os.path.join(here, 'generate-data'))
    lst = [doit2(doit(3)), doit(2)]
    x = aaron0(lst[0])
    lst += [doit(2) for _ in range(200)]
    [doit(x) for x in range(5, 10)]
    return {'list': lst, 'x': x, 'tarball': tarball, 'docker': docker}


if __name__ == '__main__':
    print('dag ==', dml.run(main, name='test'))
