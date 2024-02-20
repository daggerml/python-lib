import json
import logging
import subprocess
import traceback as tb
from dataclasses import dataclass, field, fields
from typing import Dict, List, Union

logger = logging.getLogger(__name__)

DATA_TYPE = {}
# usefull for tests

@dataclass
class CliFlags:
    # this could be just generally done better -- but it gets the job done for
    # now
    flag_dict: Dict[str, str]
    parent: Union["CliFlags", None] = None

    def update(self, kw: Dict[str, str]) -> "CliFlags":
        flags = dict(**self.flag_dict)
        flags.update(kw)
        return CliFlags(flags, parent=self)

    def revert(self) -> Union["CliFlags", None]:
        return self.parent or CliFlags({})

    def to_flags(self) -> List[str]:
        out = []
        for k, v in sorted(self.flag_dict.items()):
            out.extend([f'--{k}', v])
        return out

    def __iter__(self):
        return iter(self.to_flags())

CLI_FLAGS = CliFlags({})


def from_json(text):
    return from_data(json.loads(text))


def to_json(obj):
    return json.dumps(to_data(obj), separators=(',', ':'))


def from_data(data):
    n, *args = data if isinstance(data, list) else [None, data]
    if n is None:
        return args[0]
    if n == 'l':
        return [from_data(x) for x in args]
    if n == 's':
        return {from_data(x) for x in args}
    if n == 'd':
        return {k: from_data(v) for (k, v) in args}
    if n in DATA_TYPE:
        return DATA_TYPE[n](*[from_data(x) for x in args])
    raise ValueError(f'no data encoding for type: {n}')


def to_data(obj):
    if isinstance(obj, tuple):
        obj = list(obj)
    n = obj.__class__.__name__
    if isinstance(obj, (type(None), str, bool, int, float)):
        return obj
    if isinstance(obj, (list, set)):
        return [n[0], *[to_data(x) for x in obj]]
    if isinstance(obj, dict):
        return [n[0], *[[k, to_data(v)] for k, v in obj.items()]]
    if n in DATA_TYPE:
        return [n, *[to_data(getattr(obj, x.name)) for x in fields(obj)]]
    raise ValueError(f'no data encoding for type: {n}')


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


@dml_type
@dataclass
class Error(Exception):
    message: str
    context: dict = field(default_factory=dict)
    code: str|None = None

    def __post_init__(self):
        self.code = type(self).__name__ if self.code is None else self.code

    @classmethod
    def from_ex(cls, ex):
        if isinstance(ex, Error):
            return ex
        formatted_tb = tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)
        return cls(str(ex), {'trace': formatted_tb}, type(ex).__name__)


class ApiError(Error):
    pass


def _api(*args):
    try:
        logger.debug('cmd args: %r', ['dml', *CLI_FLAGS, *args])
        resp = subprocess.run(['dml', *CLI_FLAGS, *args], capture_output=True)
        logger.debug('response: %r', resp.stdout)
        return resp.stdout
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise ApiError.from_ex(e) from e


def invoke_api(db, op, *args, **kwargs):
    payload = to_json([op, args, kwargs])
    # FIXME  this shouldn't be to_json'd but the api expects it
    token = to_json(db)
    resp = _api('dag', 'invoke', token, payload)
    data = from_json(resp.decode())
    if isinstance(data, Error):
        raise data
    return data
