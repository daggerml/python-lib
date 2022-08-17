import dml


@dml.func
def doit(n):
    print('doit:', dml.to_py(n))
    datum = dml.from_py({'sdf': [1, 2, 3, n, ['four', 'five']]})
    print('-----', dml.to_py(n))
    return datum

doit2 = dml.func(lambda x: x['sdf'])

@dml.func
def main():
    return [doit2(doit(3)), doit(3), doit(2)]


if __name__ == '__main__':
    print(dml.from_py("asdf"))
    print('dag:', dml.run(main))
