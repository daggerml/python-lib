import daggerml as dml


@dml.func
def main():
    build_func = dml.Func('docker-build', 'build')
    return {'build': build_func}


if __name__ == '__main__':
    print('dag ==', dml.run(main, name='docker'))
