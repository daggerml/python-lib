"""Built-in function registry for the DML runtime system.

Provides the BUILTIN_FNS dictionary containing all built-in functions
that can be called during DAG execution.

Public API:
    BUILTIN_FNS - Dictionary mapping function names to implementations

Private API:
    None (this module only exports the registry)
"""

from daggerml._core.types import NONE, Runnable, Uri
from daggerml._core.util import unnest


def assoc(xs, k, v):
    """Associate a key-value pair in a dictionary."""
    if not isinstance(xs, dict):
        raise TypeError(f"Cannot assoc on object of type {type(xs).__name__}, expected dict")
    if not isinstance(k, str):
        raise TypeError("Dictionary keys must be strings")
    result = xs.copy()
    result[k] = v
    return result


def conj(xs, x):
    """Conjoin an element to a list."""
    if not isinstance(xs, list):
        raise TypeError(f"Cannot conj on object of type {type(xs).__name__}, expected list")
    return [*xs, x]


def get(x, k, d=NONE):
    """Get value from a list or dict with optional default."""
    if isinstance(x, list):
        if d is not NONE:
            raise TypeError("Default values not supported for list access")
        if isinstance(k, list):
            if len(k) != 2:
                raise ValueError("Slice key must have exactly 2 elements [start, stop]")
            if not all(isinstance(i, int) for i in k):
                raise TypeError("Slice indices must be integers")
            return x[slice(*k)]
        if not isinstance(k, int):
            raise TypeError("List indices must be integers")
        return x[k]
    if isinstance(x, dict):
        if not isinstance(k, str):
            raise TypeError(f"Dict keys must be strings but got {type(k).__name__}")
        if d is NONE:
            return x[k]
        return x.get(k, d)
    raise TypeError(f"Cannot get from object of type {type(x).__name__}, expected list or dict")


def contains(x, k):
    """Check if container contains key/value."""
    if not isinstance(x, (list, dict)):
        raise TypeError(f"Cannot check contains on object of type {type(x).__name__}, expected list or dict")
    return k in x


def make_list(*xs):
    """Create a list from arguments."""
    return list(xs)


def make_dict(*kvs):
    """Create a dict from alternating key-value arguments."""
    if len(kvs) % 2 != 0:
        raise ValueError("Dict requires an even number of arguments (key-value pairs)")
    try:
        return dict(zip(kvs[0::2], kvs[1::2], strict=True))
    except TypeError as e:
        raise TypeError("Invalid key-value pairs for dict") from e


def make_uri(uri):
    """Create a Uri datum value."""
    if isinstance(uri, Uri):
        return uri
    if not isinstance(uri, str):
        raise TypeError(f"Uri requires a string, got {type(uri).__name__}")
    return Uri(uri)


def make_runnable(target, sub, kwargs, adapter):
    """Create a runnable value."""
    if isinstance(target, str):
        target = make_uri(target)
    if not isinstance(target, Uri):
        raise TypeError(f"Runnable target must be Uri, got {type(target).__name__}")
    if kwargs is None:
        kwargs = {}
    if not isinstance(kwargs, dict):
        raise TypeError(f"Runnable kwargs must be dict, got {type(kwargs).__name__}")
    if sub is not None and not isinstance(sub, Runnable):
        raise TypeError(f"Runnable sub must be Runnable, got {type(sub).__name__}")
    if not isinstance(adapter, str):
        raise TypeError(f"Runnable adapter must be string, got {type(adapter).__name__}")
    return Runnable(target=target, sub=sub, kwargs=kwargs, adapter=adapter)


# Built-in functions available to DML computations
BUILTIN_FNS = {
    "get": get,
    "contains": contains,
    "list": make_list,
    "dict": make_dict,
    "uri": make_uri,
    "runnable": make_runnable,
    "assoc": assoc,
    "conj": conj,
    "unnest": unnest,
}
