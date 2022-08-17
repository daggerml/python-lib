import dml

@dml.func
def main():
    dkr_build = dml.load('docker')['build']
    tarball = dml.tar('./generate-data/')
    image = dkr_build(tarball)
    f = dml.Func('docker', image)
    return {'f': f, 'tarball': tarball, 'image': image}

if __name__ == '__main__':
    print('dag ==', dml.run(main, name='test'))
