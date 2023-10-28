import json
import logging
import subprocess
from dataclasses import dataclass, field, fields

logger = logging.getLogger(__name__)

DATA_TYPE = {}


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


def dml_type(cls=None, slots=True, weakref_slot=True, **kwargs):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return dataclass(slots=slots, weakref_slot=weakref_slot, **kwargs)(cls)
    return decorator(cls) if cls else decorator


@dml_type
class Error(Exception):
    message: str
    context: dict = field(default_factory=dict)
    code: str = None

    def __post_init__(self):
        self.code = type(self).__name__ if self.code is None else self.code

    @classmethod
    def from_ex(cls, ex):
        return ex if isinstance(ex, Error) else cls(str(ex), {}, type(ex).__name__)


class ApiError(Error):
    pass


def _api(*args):
    try:
        resp = subprocess.run(['dml', *args], capture_output=True, shell=True)
        if resp.returncode != 0:
            raise ApiError(resp.stderr)
        data = from_json(resp.stdout)
        if data['status'] != 'ok':
            err = data['error']
            raise err
        return data['result'], data.get('token')
    except KeyboardInterrupt:
        raise
    except Error:
        raise
    except Exception as e:
        raise ApiError.from_ex(e) from e


def invoke_api(token, op, *args, **kwargs):
    payload = to_json([op, args, kwargs])
    # FIXME  this shouldn't be to_json'd but the api expects it
    token = to_json(token)
    return _api('dag', 'invoke', token, payload)
