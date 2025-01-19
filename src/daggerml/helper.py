from dataclasses import dataclass, field
from tempfile import TemporaryDirectory

from daggerml.core import Dml as RealDml


@dataclass
class Dml:
    tmpdirs: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    dml: RealDml = field(default_factory=RealDml)

    @classmethod
    def init(cls, **kwargs):
        tmpdirs = [TemporaryDirectory() for _ in range(2)]
        kwargs.setdefault('config_dir', tmpdirs[0].__enter__())
        kwargs.setdefault('project_dir', tmpdirs[1].__enter__())
        kwargs.setdefault('repo', 'test')
        kwargs.setdefault('user', 'test')
        kwargs.setdefault('branch', 'main')
        dml = cls(tmpdirs, kwargs, RealDml.init(**kwargs))
        if kwargs['repo'] not in [x['name'] for x in dml('repo', 'list')]:
            dml('repo', 'create', kwargs['repo'])
        if kwargs['branch'] not in dml('branch', 'list'):
            dml('branch', 'create', kwargs['branch'])
        return dml

    def __call__(self, *args, **kwargs):
        return self.dml(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.dml, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        [x.__exit__(*args) for x in self.tmpdirs]
