from daggerml.util import Resource, Func, func, run, load, to_py, tar
import pkg_resources  # part of setuptools

__version__ = pkg_resources.require('daggerml')[0].version
del pkg_resources

__all__ = ('Resource', 'Func', 'func', 'run', 'load', 'to_py', 'tar')
