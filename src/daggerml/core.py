import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field, fields
from os import getenv
from tempfile import TemporaryDirectory
from traceback import format_exception
from typing import Any, Callable, NewType, Optional, Union

from daggerml.util import (
    BackoffWithJitter,
    current_time_millis,
    kwargs2opts,
    postwalk,
    properties,
    raise_ex,
    replace,
    setter,
)

log = logging.getLogger(__name__)

DATA_TYPE = {}

Node = NewType("Node", None)
Resource = NewType("Resource", None)
Error = NewType("Error", None)
Ref = NewType("Ref", None)
Dml = NewType("Dml", None)
Dag = NewType("Dag", None)
Scalar = Union[str, int, float, bool, type(None), Resource, Node]
Collection = Union[list, tuple, set, dict]


def dml_type(cls=None, **opts):
    def decorator(cls):
        DATA_TYPE[opts.get("alias", None) or cls.__name__] = cls
        return cls

    return decorator(cls) if cls else decorator


def from_data(data):
    n, *args = data if isinstance(data, list) else [None, data]
    if n is None:
        return args[0]
    if n == "l":
        return [from_data(x) for x in args]
    if n == "s":
        return {from_data(x) for x in args}
    if n == "d":
        return {k: from_data(v) for (k, v) in args}
    if n in DATA_TYPE:
        return DATA_TYPE[n](*[from_data(x) for x in args])
    raise ValueError(f"no decoder for type: {n}")


def to_data(obj):
    if isinstance(obj, Node):
        obj = obj.ref
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
    raise ValueError(f"no encoder for type: {n}")


def from_json(text):
    """
    Parse JSON string into Python objects.

    Parameters
    ----------
    text : str
        JSON string to parse

    Returns
    -------
    Any
        Deserialized Python object
    """
    return from_data(json.loads(text))


def to_json(obj):
    """
    Convert Python object to JSON string.

    Parameters
    ----------
    obj : Any
        Object to serialize

    Returns
    -------
    str
        JSON string representation
    """
    return json.dumps(to_data(obj), separators=(",", ":"))


@dml_type
@dataclass(frozen=True)
class Ref:  # noqa: F811
    """
    Reference to a DaggerML node.

    Parameters
    ----------
    to : str
        Reference identifier
    """

    to: str


@dml_type
@dataclass(frozen=True)
class Resource:  # noqa: F811
    """
    Representation of an external resource.

    Parameters
    ----------
    uri : str
        Resource URI
    data : str, optional
        Associated data
    adapter : str, optional
        Resource adapter name
    """

    uri: str
    data: Optional[str] = None
    adapter: Optional[str] = None


@dml_type
@dataclass
class Error(Exception):  # noqa: F811
    """
    Custom error type for DaggerML.

    Parameters
    ----------
    message : Union[str, Exception]
        Error message or exception
    context : dict, optional
        Additional error context
    code : str, optional
        Error code
    """

    message: Union[str, Exception]
    context: dict = field(default_factory=dict)
    code: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.message, Error):
            ex = self.message
            self.message = ex.message
            self.context = ex.context
            self.code = ex.code
        elif isinstance(self.message, Exception):
            ex = self.message
            self.message = str(ex)
            self.context = {"trace": format_exception(type(ex), value=ex, tb=ex.__traceback__)}
            self.code = type(ex).__name__
        else:
            self.code = type(self).__name__ if self.code is None else self.code

    def __str__(self):
        return "".join(self.context.get("trace", [self.message]))


class Dml:  # noqa: F811
    """
    Main DaggerML interface for creating and managing DAGs.

    Parameters
    ----------
    data : Any, optional
        Initial data for the DML instance
    message_handler : callable, optional
        Function to handle messages
    **kwargs : dict
        Additional configuration options

    Examples
    --------
    >>> from daggerml import Dml
    >>> with Dml() as dml:
    ...     with dml.new("d0", "message") as dag:
    ...         pass
    """

    def __init__(self, *, data=None, message_handler=None, **kwargs):
        self.data = data
        self.message_handler = message_handler
        self.kwargs = kwargs
        self.opts = kwargs2opts(**kwargs)
        self.token = None
        self.tmpdirs = None
        self.cache_key = None
        self.dump = None

    def __call__(self, *args: str, as_text: bool = False) -> Any:
        """
        Call the dml cli with the given arguments.

        Parameters
        ----------
        *args : str
            Arguments to pass to the dml cli
        as_text : bool, optional
            If True, return the result as text, otherwise json

        Returns
        -------
        Any
            Result of the execution

        Examples
        -----
        >>> dml = Dml()
        >>> _ = dml("repo", "list")

        is equivalent to `dml repo list`.
        """
        resp = None
        path = shutil.which("dml")
        argv = [path, *self.opts, *args]
        resp = subprocess.run(argv, check=True, capture_output=True, text=True)
        if resp.stderr:
            log.error(resp.stderr.rstrip())
        try:
            resp = resp.stdout or "" if as_text else json.loads(resp.stdout or "null")
        except json.decoder.JSONDecodeError:
            pass
        return resp

    def __getattr__(self, name: str):
        def invoke(*args, **kwargs):
            opargs = to_json([name, args, kwargs])
            token = self.token or to_json([])
            return raise_ex(from_data(self("api", "invoke", token, opargs)))

        return invoke

    def __enter__(self):
        "Use temporary config and project directories."
        self.tmpdirs = [TemporaryDirectory() for _ in range(2)]
        self.kwargs = {
            "config_dir": getenv("DML_CONFIG_DIR") or self.tmpdirs[0].__enter__(),
            "project_dir": getenv("DML_PROJECT_DIR") or self.tmpdirs[1].__enter__(),
            "repo": getenv("DML_REPO") or "test",
            "user": getenv("DML_USER") or "test",
            "branch": getenv("DML_BRANCH") or "main",
            **self.kwargs,
        }
        self.opts = kwargs2opts(**self.kwargs)
        self.cache_key, self.dump = from_json(self.data or to_json([None, None]))
        if self.kwargs["repo"] not in [x["name"] for x in self("repo", "list")]:
            self("repo", "create", self.kwargs["repo"])
        if self.kwargs["branch"] not in self("branch", "list"):
            self("branch", "create", self.kwargs["branch"])
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        [x.__exit__(exc_type, exc_value, traceback) for x in self.tmpdirs]
        if exc_value and self.message_handler:
            self.message_handler(to_json(Error(exc_value)))

    @property
    def envvars(self):
        return {f"DML_{k.upper()}": str(v) for k, v in self.kwargs.items()}

    def new(self, name: str, message: str) -> Dag:
        """
        Create a new DAG.

        Parameters
        ----------
        name : str
            Name of the DAG
        message : str
            Description or commit message

        Returns
        -------
        Dag
            New Dag instance

        Examples
        --------
        >>> with dml.new("dag name", "message") as dag:
        ...     pass
        """
        opts = [] if not self.dump else kwargs2opts(dump=self.dump)
        token = self("api", "create", *opts, name, message, as_text=True)
        return Dag(replace(self, token=token), self.dump, self.message_handler)

    def load(self, name: Union[str, Node]) -> Dag:
        return Dag(replace(self, token=None), None, _ref=self.get_dag(name))


@dataclass
class Boxed:
    value: Any


@dataclass
class Dag:  # noqa: F811
    """
    Representation of a DaggerML DAG.

    Parameters
    ----------
    _dml : Dml
        DaggerML instance
    _dump : str, optional
        Serialized DAG data.
    _message_handler : callable, optional
        Function to handle messages.
    _init_complete : bool, optional
        True when object initialization is complete.
    """

    _dml: Dml
    _dump: Optional[str] = None
    _message_handler: Optional[Callable] = None
    _init_complete: bool = False
    _ref: Optional[Ref] = None

    def __post_init__(self):
        self._init_complete = True

    def __hash__(self):
        "Useful only for tests."
        return 42

    def __enter__(self):
        "Catch exceptions and commit an Error"
        assert not self._ref
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self._commit(Error(exc_value))
        if self._dump and self._message_handler:
            self._message_handler(self._dump)

    def __getitem__(self, name):
        return Node(self, self._dml.get_node(name, self._ref))

    def __setitem__(self, name, value):
        assert not self._ref
        if isinstance(value, Ref):
            return self._dml.set_node(name, value)
        return self._put(value, name=name)

    def __len__(self):
        return len(self._dml.get_names(self._ref))

    def __iter__(self):
        for k in self.keys():
            yield k

    def __setattr__(self, name, value):
        priv = name.startswith("_")
        flds = name in {x.name for x in fields(self)}
        prps = name in properties(self)
        init = not self._init_complete
        boxd = isinstance(value, Boxed)
        if (flds and init) or (not self._ref and ((not flds and not priv) or prps or boxd)):
            value = value.value if boxd else value
            if flds or (prps and setter(self, name)):
                return super(Dag, self).__setattr__(name, value)
            elif not prps:
                return self.__setitem__(name, value)
        raise AttributeError(f"can't set attribute: '{name}'")

    def __getattr__(self, name):
        return self.__getitem__(name)

    @property
    def argv(self) -> Node:
        "Access the dag's argv node"
        return Node(self, self._dml.get_argv(self._ref))

    @property
    def result(self) -> Node:
        ref = self._dml.get_result(self._ref)
        assert ref, f"'{self.__class__.__name__}' has no attribute 'result'"
        return Node(self, ref) if ref else ref

    @result.setter
    def result(self, value):
        return self._commit(value)

    @property
    def keys(self) -> list[str]:
        return lambda: self._dml.get_names(self._ref).keys()

    @property
    def values(self) -> list[Node]:
        def result():
            nodes = self._dml.get_names(self._ref).values()
            return [Node(self, x) for x in nodes]

        return result

    def _put(self, value: Union[Scalar, Collection], *, name=None, doc=None) -> Node:
        """
        Add a value to the DAG.

        Parameters
        ----------
        value : Union[Scalar, Collection]
            Value to add
        name : str, optional
            Name for the node
        doc : str, optional
            Documentation

        Returns
        -------
        Node
            Node representing the value
        """
        value = postwalk(
            value,
            lambda x: isinstance(x, Node) and x.dag._ref,
            lambda x: self._load(x.dag, x.ref),
        )
        return Node(self, self._dml.put_literal(value, name=name, doc=doc))

    def _load(self, dag_name, node=None, *, name=None, doc=None) -> Node:
        """
        Load a DAG by name.

        Parameters
        ----------
        dag_name : str
            Name of the DAG to load
        name : str, optional
            Name for the node
        doc : str, optional
            Documentation

        Returns
        -------
        Node
            Node representing the loaded DAG
        """
        dag = dag_name if isinstance(dag_name, str) else dag_name._ref
        return Node(self, self._dml.put_load(dag, node, name=name, doc=doc))

    def _commit(self, value) -> Node:
        """
        Commit a value to the DAG.

        Parameters
        ----------
        value : Union[Node, Error, Any]
            Value to commit
        """
        value = value if isinstance(value, (Node, Error)) else self._put(value)
        self._dump = Boxed(self._dml.commit(value))
        self._ref = Boxed(Ref(json.loads(self._dump)[-1][1][1]))


@dataclass(frozen=True)
class Node:  # noqa: F811
    """
    Representation of a node in a DaggerML DAG.

    Parameters
    ----------
    dag : Dag
        Parent DAG
    ref : Ref
        Node reference
    """

    dag: Dag
    ref: Ref

    def __repr__(self):
        ref_id = self.ref if isinstance(self.ref, Error) else self.ref.to
        return f"{self.__class__.__name__}({ref_id})"

    def __hash__(self):
        return hash(self.ref)

    def __getitem__(self, key: Union[slice, str, int, Node]) -> Node:
        """
        Get the `key` item. It should be the same as if you were working on the
        actual value.

        Returns
        -------
        Node
            Node with the length of the collection

        Raises
        ------
        Error
            If the node isn't a collection (e.g. list, set, or dict).

        Examples
        --------
        >>> node = dag._put({"a": 1, "b": 5})
        >>> assert node["a"].value() == 1
        """
        if isinstance(key, slice):
            key = [key.start, key.stop, key.step]
        return Node(self.dag, self.dag._dml.get(self, key))

    def __len__(self):  # python requires this to be an int
        """
        Get the node's length

        Returns
        -------
        Node
            Node with the length of the collection

        Raises
        ------
        Error
            If the node isn't a collection (e.g. list, set, or dict).
        """
        result = self.len().value()
        assert isinstance(result, int)
        return result

    def __iter__(self):
        """
        Iterate over the node's values (items if it's a list, and keys if it's a
        dict)

        Returns
        -------
        Node
            Result node

        Raises
        ------
        Error
            If the node isn't a collection (e.g. list, set, or dict).
        """
        if self.type().value() == "list":
            for i in range(len(self)):
                yield self[i]
        elif self.type().value() == "dict":
            for k in self.keys():
                yield k

    def __call__(self, *args, name=None, doc=None, sleep=None, timeout=0) -> Node:
        """
        Call this node as a function.

        Parameters
        ----------
        *args : Any
            Arguments to pass to the function
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation
        timeout : int, default=30000
            Maximum time to wait in milliseconds

        Returns
        -------
        Node
            Result node

        Raises
        ------
        TimeoutError
            If the function call exceeds the timeout
        Error
            If the function returns an error
        """
        sleep = sleep or BackoffWithJitter()
        args = [self.dag._put(x) for x in args]
        end = current_time_millis() + timeout
        while timeout <= 0 or current_time_millis() < end:
            resp = self.dag._dml.start_fn([self, *args], name=name, doc=doc)
            if resp:
                return Node(self.dag, resp)
            time.sleep(sleep() / 1000)
        raise TimeoutError(f"invoking function: {self.value()}")

    def keys(self, *, name=None, doc=None) -> Node:
        """
        Get the keys of a dictionary node.

        Parameters
        ----------
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation

        Returns
        -------
        Node
            Node containing the dictionary keys
        """
        return Node(self.dag, self.dag._dml.keys(self, name=name, doc=doc))

    def len(self, *, name=None, doc=None) -> Node:
        """
        Get the length of a collection node.

        Parameters
        ----------
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation

        Returns
        -------
        Node
            Node containing the length
        """
        return Node(self.dag, self.dag._dml.len(self, name=name, doc=doc))

    def type(self, *, name=None, doc=None) -> Node:
        """
        Get the type of this node.

        Parameters
        ----------
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation

        Returns
        -------
        Node
            Node containing the type information
        """
        return Node(self.dag, self.dag._dml.type(self, name=name, doc=doc))

    def get(self, key, default=None, *, name=None, doc=None):
        """
        For a dict node, return the value for key if key exists, else default.

        If default is not given, it defaults to None, so that this method never raises a KeyError.
        """
        return Node(self.dag, self.dag._dml.get(self, key, default, name=name, doc=doc))

    def items(self):
        """
        Iterate over key-value pairs of a dictionary node.

        Returns
        -------
        Iterator[tuple[Node, Node]]
            Iterator over (key, value) pairs
        """
        for k in self:
            yield k, self[k]

    def value(self):
        """
        Get the concrete value of this node.

        Returns
        -------
        Any
            The actual value represented by this node
        """
        return self.dag._dml.get_node_value(self.ref)
