from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Any

from daggerml.core import Dag as RealDag
from daggerml.core import Dml as RealDml
from daggerml.core import Error, from_json, to_json


@dataclass
class Dag(RealDag):
    message_handler: Any | None = None

    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        if self.dump and self.message_handler:
            self.message_handler(self.dump)


class Dml(RealDml):
    def __init__(self, data=None, message_handler=print, **kwargs):
        tmpdirs = [TemporaryDirectory() for _ in range(2)]
        super().__init__(**{**{
            'config_dir': tmpdirs[0].__enter__(),
            'project_dir': tmpdirs[1].__enter__(),
            'repo': 'test',
            'user': 'test',
            'branch': 'main',
        }, **kwargs})
        self.data = data
        self.message_handler = message_handler
        self.tmpdirs = tmpdirs
        ref, self.dag_dump = from_json(data or to_json([None, None]))
        self.cache_key = ref.to.split('/', 1).pop() if ref else None
        if self.kwargs['repo'] not in [x['name'] for x in self('repo', 'list')]:
            self('repo', 'create', self.kwargs['repo'])
        if self.kwargs['branch'] not in self('branch', 'list'):
            self('branch', 'create', self.kwargs['branch'])

    def new(self, *args, **kwargs):
        props = super().new(*args, dag_dump=self.dag_dump, **kwargs).__dict__
        return Dag(**props, message_handler=self.message_handler)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        [x.__exit__(exc_type, exc_value, traceback) for x in self.tmpdirs]
        if exc_value and self.message_handler:
            self.message_handler(to_json(Error(exc_value)))
