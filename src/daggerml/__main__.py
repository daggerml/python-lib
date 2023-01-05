from daggerml.cli import cli
import sys


if __name__ == '__main__':
    cli(sys.argv[1:])
else:
    raise RuntimeError('Do not import %s directly' % __name__)
