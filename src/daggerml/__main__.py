from daggerml._cli import cli
import sys


if __name__ == '__main__':
    cli()
else:
    raise RuntimeError('Do not import %s directly' % __name__)
