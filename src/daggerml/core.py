import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field, fields
from tempfile import TemporaryDirectory
from traceback import format_exception
from typing import Any, Callable, Optional, Union

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

Scalar = Union[str, int, float, bool, type(None), "Resource", "Node"]
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
    return from_data(json.loads(text))


def to_json(obj):
    return json.dumps(to_data(obj), separators=(",", ":"))


@dml_type
@dataclass(frozen=True)
class Ref:  # noqa: F811
    """
    Reference to a DaggerML object.

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
    Representation of an externally managed object with an identifier.

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


@dataclass
class Dml:
    """
    DaggerML cli client wrapper
    """

    config_dir: Union[str, None] = None
    project_dir: Union[str, None] = None
    cache_path: Union[str, None] = None
    repo: Union[str, None] = None
    user: Union[str, None] = None
    branch: Union[str, None] = None
    token: Union[str, None] = None
    tmpdirs: dict[str, TemporaryDirectory] = field(default_factory=dict)

    @property
    def kwargs(self) -> dict:
        out = {
            "config_dir": self.config_dir,
            "project_dir": self.project_dir,
            "cache_path": self.cache_path,
            "repo": self.repo,
            "user": self.user,
            "branch": self.branch,
        }
        return {k: v for k, v in out.items() if v is not None}

    @classmethod
    def temporary(cls, repo="test", user="user", branch="main", cache_path=None, **kwargs) -> "Dml":
        """
        Create a temporary Dml instance with specified parameters.

        Parameters
        ----------
        repo : str, default="test"
        user : str, default="user"
        branch : str, default="main"
        **kwargs : dict
            Additional keyword arguments for configuration include `config_dir`, `project_dir`, and `cache_path`.
            If any of those is provided, it will not create a temporary directory for that parameter. If provided and
            set to None, the dml default will be used.
        """
        tmpdirs = {k: TemporaryDirectory(prefix="dml-") for k in ["config_dir", "project_dir"] if k not in kwargs}
        self = cls(
            repo=repo,
            user=user,
            branch=branch,
            cache_path=cache_path,
            **{k: v.name for k, v in tmpdirs.items()},
            tmpdirs=tmpdirs,
        )
        if self.kwargs["repo"] not in [x["name"] for x in self("repo", "list")]:
            self("repo", "create", self.kwargs["repo"])
        return self

    def cleanup(self):
        [x.cleanup() for x in self.tmpdirs.values()]

    def __call__(self, *args: str, input=None, as_text: bool = False) -> Any:
        path = shutil.which("dml")
        argv = [path, *kwargs2opts(**self.kwargs), *args]
        resp = subprocess.run(argv, check=False, capture_output=True, text=True, input=input)
        if resp.returncode != 0:
            raise_ex(Error(resp.stderr or "DML command failed", code="DmlError"))
        log.debug("dml command stderr: %s", resp.stderr)
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
            return raise_ex(from_data(self("api", "invoke", token, input=opargs)))

        return invoke

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    @property
    def envvars(self):
        return {f"DML_{k.upper()}": str(v) for k, v in self.kwargs.items()}

    def new(self, name="", message="", data=None, message_handler=None) -> "Dag":
        opts = kwargs2opts(dump="-") if data else []
        token = self("api", "create", *opts, name, message, input=data, as_text=True)
        return Dag(replace(self, token=token), message_handler)

    def load(self, name: Union[str, "Node"], recurse=False) -> "Dag":
        return Dag(replace(self, token=None), _ref=self.get_dag(name, recurse=recurse))


@dataclass
class Boxed:
    value: Any


@dataclass
class Dag:
    _dml: Dml
    _message_handler: Optional[Callable] = None
    _ref: Optional[Ref] = None
    _init_complete: bool = False

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

    def __getitem__(self, name) -> "Node":
        return Node(self, self._dml.get_node(name, self._ref))

    def __setitem__(self, name, value) -> "Node":
        assert not self._ref
        if isinstance(value, Ref):
            return self._dml.set_node(name, value)
        return self._put(value, name=name)

    def __len__(self) -> int:
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
    def argv(self) -> "Node":
        "Access the dag's argv node"
        return Node(self, self._dml.get_argv(self._ref))

    @property
    def result(self) -> "Node":
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
    def values(self) -> list["Node"]:
        def result():
            nodes = self._dml.get_names(self._ref).values()
            return [Node(self, x) for x in nodes]

        return result

    def _put(self, value: Union[Scalar, Collection], *, name=None, doc=None) -> "Node":
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

    def _load(self, dag_name, node=None, *, name=None, doc=None) -> "Node":
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

    def _commit(self, value) -> "Node":
        """
        Commit a value to the DAG.

        Parameters
        ----------
        value : Union[Node, Error, Any]
            Value to commit
        """
        value = value if isinstance(value, (Node, Error)) else self._put(value)
        dump = self._dml.commit(value)
        if self._message_handler:
            self._message_handler(dump)
        self._ref = Boxed(Ref(json.loads(dump)[-1][1][1]))


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

    @property
    def argv(self) -> "Node":
        "Access the node's argv list"
        return [Node(self.dag, x) for x in self.dag._dml.get_argv(self)]

    def load(self, *keys: Union[str, int]) -> Dag:
        """
        Convenience wrapper around `dml.load(node)`

        If `key` is provided, it considers this node to be a collection created
        by the appropriate method and loads the dag that corresponds to this key

        Parameters
        ----------
        *keys : str, optional
            Key to load from the DAG. If not provided, the entire DAG is loaded.

        Returns
        -------
        Dag
            The dag that this node was imported from (or in the case of a function call, this returns the fndag)

        Examples
        --------
        >>> dml = Dml.temporary()
        >>> dag = dml.new("test", "test")
        >>> l0 = dag._put(42)
        >>> c0 = dag._put({"a": 1, "b": [l0, "23"]})
        >>> assert c0.load("b", 0) == l0
        >>> assert c0.load("b").load(0) == l0
        >>> assert c0["b"][0] != l0  # this is a different node, not the same as l0
        >>> dml.cleanup()
        """
        if len(keys) == 0:
            return self.dag._dml.load(self)
        data = self.dag._dml("node", "backtrack", self.ref.to, *map(str, keys))
        return Node(self.dag, from_data(data))

    def __getitem__(self, key: Union[slice, str, int, "Node"]) -> "Node":
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
        >>> dml = Dml.temporary()
        >>> dag = dml.new("test", "test")
        >>> node = dag._put({"a": 1, "b": [5, 6]})
        >>> nested = node["a"]
        >>> isinstance(nested, Node)
        True
        >>> nested.value()
        1
        >>> node["b"][0].value()  # lists too
        5
        """
        if isinstance(key, slice):
            key = [key.start, key.stop, key.step]
        return Node(self.dag, self.dag._dml.get(self, key))

    def contains(self, item, *, name=None, doc=None):
        """
        For collection nodes, checks to see if `item` is in `self`

        Returns
        -------
        Node
            Node with the boolean of is `item` in `self`
        """
        return Node(self.dag, self.dag._dml.contains(self, item, name=name, doc=doc))

    def __contains__(self, item):
        return self.contains(item).value()  # has to return boolean

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

    def __call__(self, *args, name=None, doc=None, sleep=None, timeout=0) -> "Node":
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
        sleep : callable, optional
            A nullary function that returns sleep time in milliseconds
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

    def keys(self, *, name=None, doc=None) -> "Node":
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

    def len(self, *, name=None, doc=None) -> "Node":
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

    def type(self, *, name=None, doc=None) -> "Node":
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

    def assoc(self, key, value, *, name=None, doc=None):
        """
        For a dict node, associate a new value into the map

        Returns
        -------
        Node
            Node containing the new dict
        """
        return Node(self.dag, self.dag._dml.assoc(self, key, value, name=name, doc=doc))

    def conj(self, item, *, name=None, doc=None):
        """
        For a list or set node, append an item

        Returns
        -------
        Node
            Node containing the new collection

        Notes
        -----
        `append` is an alias `conj`
        """
        return Node(self.dag, self.dag._dml.conj(self, item, name=name, doc=doc))

    def append(self, item, *, name=None, doc=None):
        """
        For a list or set node, append an item

        Returns
        -------
        Node
            Node containing the new collection

        See Also
        --------
        conj : The main implementation
        """
        return self.conj(item, name=name, doc=doc)

    def update(self, update):
        """
        For a dict node, update like python dicts

        Returns
        -------
        Node
            Node containing the new collection

        Notes
        -----
        calls `assoc` iteratively for k, v pairs in update.

        See Also
        --------
        assoc : The main implementation
        """
        for k, v in update.items():
            self = self.assoc(k, v)
        return self
