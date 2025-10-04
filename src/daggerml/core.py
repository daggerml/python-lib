import json
import logging
import shutil
import subprocess
import time
import traceback as tb
from dataclasses import dataclass, field, fields
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Iterator, Optional, Union, cast, overload

from daggerml.util import BackoffWithJitter, current_time_millis, kwargs2opts, raise_ex, replace

log = logging.getLogger(__name__)

DATA_TYPE = {}

Scalar = Union[str, int, float, bool, type(None), "Resource", "Executable"]
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
@dataclass
class Resource:  # noqa: F811
    """
    Representation of an externally managed object with an identifier.

    Parameters
    ----------
    uri : str
        Resource URI
    """

    uri: str


@dml_type
@dataclass
class Executable(Resource):  # noqa: F811
    """
    Representation of an executable externally managed object with an identifier.

    Parameters
    ----------
    uri : str
        Resource URI
    data : str, optional
        Associated data
    adapter : str, optional
        Adapter cli script
    """

    data: dict = field(default_factory=dict)
    adapter: Optional[str] = None
    prepop: Dict[str, Union["Node", Scalar, Collection]] = field(default_factory=dict)


@dml_type
@dataclass
class Error(Exception):
    message: str
    origin: str
    type: str
    stack: list[dict] = field(default_factory=list)

    @classmethod
    def from_ex(cls, ex: BaseException) -> "Error":
        if isinstance(ex, Error):
            return ex
        return cls(
            message=str(ex),
            origin="python",
            type=ex.__class__.__name__,
            stack=[
                {
                    "filename": frame.filename,
                    "function": frame.name,
                    "lineno": frame.lineno,
                    "line": (frame.line or "").strip(),
                }
                for frame in tb.extract_tb(ex.__traceback__)
            ],
        )

    def __str__(self):
        lines = [f"Traceback (most recent call last) from {self.origin}:\n"]
        for frame in self.stack:
            lines.append(f'  File "{frame["filename"]}", line {frame["lineno"]}, in {frame["function"]}\n')
            if "line" in frame and frame["line"]:
                lines.append(f"    {frame['line']}\n")
        lines.append(f"{self.type}: {self.message}")
        return "".join(lines)


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
    def index(self) -> Optional[str]:
        if self.token:
            return json.loads(self.token)[-1]

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
            raise_ex(Error(resp.stderr or "DML command failed", origin="dml", type="CliError"))
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
        return Dag(replace(self, token=None), ref=self.get_dag(name, recurse=recurse))


def make_node(dag: "Dag", ref: Ref) -> "Node":
    """
    Create a Node from a Dag and Ref.

    Parameters
    ----------
    dag : Dag
        The parent DAG.
    ref : Ref
        The reference to the node.

    Returns
    -------
    Node
        A Node instance representing the reference in the DAG.
    """
    info = dag.dml("node", "describe", ref.to)
    if info["data_type"] == "list":
        return ListNode(dag, ref, _info=info)
    if info["data_type"] == "dict":
        return DictNode(dag, ref, _info=info)
    if info["data_type"] == "set":
        return ListNode(dag, ref, _info=info)
    if info["data_type"] == "executable":
        return ExecutableNode(dag, ref, _info=info)
    return ScalarNode(dag, ref, _info=info)


@dataclass
class Dag:
    dml: Dml
    message_handler: Optional[Callable] = None
    ref: Optional[Ref] = None

    def __repr__(self):
        to = self.ref.to if self.ref else self.dml.index or "NA"
        return f"Dag({to})"

    def __hash__(self):
        "Useful only for tests."
        return 42

    def __enter__(self):
        "Catch exceptions and commit an Error"
        assert not self.ref
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self.commit(Error.from_ex(exc_value))

    def __getitem__(self, name):
        return make_node(self, self.dml.get_node(name, self.ref))

    def __setitem__(self, name, value):
        assert not self.ref
        if isinstance(value, Ref):
            return self.dml.set_node(name, value)
        return self.put(value, name=name)

    def __setattr__(self, name, value):
        if name in [x.name for x in fields(self.__class__)]:
            return super().__setattr__(name, value)
        return self.__setitem__(name, value)

    def __getattr__(self, name):
        if name in [x.name for x in fields(self.__class__)]:
            return super().__getattribute__(name)
        return self.__getitem__(name)

    def __len__(self) -> int:
        return len(self.dml.get_names(self.ref))

    def __iter__(self):
        yield from self.keys()

    def keys(self) -> list[str]:
        """Get the list of all node names in the dag"""
        return self.dml.get_names(self.ref).keys()

    def values(self) -> list["Node"]:
        """Get the list of all nodes in the dag"""
        nodes = self.dml.get_names(self.ref).values()
        return [make_node(self, x) for x in nodes]

    @property
    def argv(self) -> "ListNode":
        "Access the dag's argv node"
        return make_node(self, self.dml.get_argv(self.ref))

    @property
    def result(self) -> "Node":
        """Get the result node of the dag"""
        ref = self.dml.get_result(self.ref)
        assert isinstance(ref, Ref), f"'{self.__class__.__name__}' dag has not been committed yet"
        return make_node(self, ref)

    def load(self, dag_name: str, key: str = "result", *, name=None, doc=None) -> "Node":
        """Load a node from a different dag into this one

        Parameters
        ----------
        dag_name : str
            Name of the dag to load
        key : str, default="result"
            The name of the node (or "result") to import from the loaded dag. By default, it imports the result node.
        name : str, optional
            Name to assign the resulting node in this dag
        doc : str, optional
            Documentation for the node

        Returns
        -------
        Node
            Import Node representing the result of the loaded dag

        Examples
        --------
        >>> dml = Dml.temporary()
        >>> dml.new("my-dag-0", "going to import this").commit(42)
        >>> dag = dml.new("my-dag-1", "importing my-dag-0")
        >>> node = dag.load("my-dag-0")
        >>> node.value()
        42
        """
        resp = getattr(self.dml.load(dag_name), key, None)
        if resp is None:
            raise_ex(Error(f"dag '{dag_name}' has no '{key}'", origin="dml", type="KeyError"))
        return self.put(resp, name=name, doc=doc)

    @overload
    def put(self, value: Union[list, set, "ListNode"], *, name=None, doc=None) -> "ListNode": ...
    @overload
    def put(self, value: Union[dict, "DictNode"], *, name=None, doc=None) -> "DictNode": ...
    @overload
    def put(self, value: Union[Executable, "ExecutableNode"], *, name=None, doc=None) -> "ExecutableNode": ...
    @overload
    def put(self, value: Union[Scalar, "ScalarNode"], *, name=None, doc=None) -> "ScalarNode": ...
    @overload
    def put(self, value: "Node", *, name=None, doc=None) -> "Node": ...
    def put(self, value: Union[Scalar, Collection, "Node"], *, name=None, doc=None) -> "Node":
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

        Examples
        --------
        >>> dml = Dml.temporary()
        >>> dag = dml.new("test", "test")
        >>> n1 = dag.put(42, name="answer", doc="the answer to life, the universe, and everything")
        >>> n1.value()
        42
        >>> n2 = dag.put({"a": 1, "b": [n1, "23"]})
        >>> n2.value()
        {'a': 1, 'b': [42, '23']}
        >>> dml.new("other-dag", "we'll import from here").commit(308)  # create and commit another dag to import
        >>> n3 = dag.load("other-dag")
        >>> n3.value()
        308
        """
        if isinstance(value, Node) and value.dag != self:
            return make_node(self, self.dml.put_load(value.dag.ref, value.ref, name=name, doc=doc))
        return make_node(self, self.dml.put_literal(value, name=name, doc=doc))

    def call(
        self,
        fn: Union[Executable, "ExecutableNode"],
        *args: Union["Node", Scalar, Collection],
        name: Optional[str] = None,
        doc: Optional[str] = None,
        sleep: Optional[callable] = None,
        timeout: int = -1,
    ) -> "Node":
        """
        Call a function node with arguments.

        Parameters
        ----------
        fn : Union[Executable, ExecutableNode]
            Function to call
        *args : Union[Node, Scalar, Collection]
            Arguments to pass to the function
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation
        sleep : callable, optional
            A nullary function that returns sleep time in milliseconds
        timeout : int, default=-1
            Maximum time to wait in milliseconds. If <= 0, wait indefinitely.

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
        expr = [self.put(x) for x in [fn, *args]]
        end = current_time_millis() + timeout
        while timeout <= 0 or current_time_millis() < end:
            resp = self.dml.start_fn(expr, name=name, doc=doc)
            if resp:
                return make_node(self, resp)
            time.sleep(sleep() / 1000)
        raise TimeoutError(f"invoking function: {expr[0].value()}")

    def commit(self, value) -> None:
        """
        Commit a value to the DAG.

        Parameters
        ----------
        value : Union[Node, Error, Any]
            Value to commit
        """
        value = value if isinstance(value, (Node, Error)) else self.put(value)
        ref = cast(Ref, self.dml.commit(value))
        if self.message_handler:
            self.message_handler(self.dml("ref", "dump", to_json(ref), as_text=True))
        self.ref = ref


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
    _info: dict = field(default_factory=dict)

    def __repr__(self):
        ref_id = self.ref if isinstance(self.ref, Error) else self.ref.to
        return f"{self.__class__.__name__}({ref_id})"

    def __hash__(self):
        return hash(self.ref)

    @property
    def argv(self) -> "Node":
        "Access the node's argv list"
        return [make_node(self.dag, x) for x in self.dag.dml.get_argv(self)]

    def backtrack(self, *keys: Union[str, int]) -> "Node":
        """
        If `key` is provided, it considers this node to be a collection created
        by the appropriate method and loads the dag that corresponds to this key

        Parameters
        ----------
        *keys : str, optional
            Keys to backtrack through the node's structure

        Returns
        -------
        Dag
            The dag that this node was imported from (or in the case of a function call, this returns the fndag)

        Examples
        --------
        >>> dml = Dml.temporary()
        >>> dag = dml.new("test", "test")
        >>> l0 = dag.put(42)
        >>> c0 = dag.put({"a": 1, "b": [l0, "23"]})
        >>> assert c0.backtrack("b", 0) == l0
        >>> assert c0.backtrack("b").backtrack(0) == l0
        >>> assert c0["b"][0] != l0  # this is a different node, not the same as l0
        >>> dml.cleanup()
        """
        data = self.dag.dml("node", "backtrack", self.ref.to, *map(str, keys))
        return make_node(self.dag, from_data(data))

    def load(self) -> Dag:
        """
        Convenience wrapper around `dml.load(node)`

        Returns
        -------
        Dag
            The dag that this node was imported from (or in the case of a function call, this returns the fndag)
        """
        return self.dag.dml.load(self)

    @property
    def type(self):
        """Get the data type of the node."""
        return self._info["data_type"]

    @overload
    def value(self: "ScalarNode") -> Scalar: ...
    @overload
    def value(self: "ListNode") -> list: ...
    @overload
    def value(self: "DictNode") -> dict: ...
    @overload
    def value(self: "ExecutableNode") -> Executable: ...
    @overload
    def value(self: "Node") -> Any: ...
    def value(self):
        """
        Get the concrete value of this node.

        Returns
        -------
        Any
            The actual value represented by this node
        """
        return self.dag.dml.get_node_value(self.ref)


class ScalarNode(Node):
    pass


class ExecutableNode(Node):
    def __call__(self, *args, name=None, doc=None, sleep=None, timeout=-1) -> "Node":
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
        timeout : int, default=-1
            Maximum time to wait in milliseconds. -1 means wait forever.

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
        return self.dag.call(self, *args, name=name, doc=doc, sleep=sleep, timeout=timeout)


class CollectionNode(Node):  # noqa: F811
    """
    Representation of a collection node in a DaggerML DAG.

    Parameters
    ----------
    dag : Dag
        Parent DAG
    ref : Ref
        Node reference
    """

    @overload
    def __getitem__(self, key: slice) -> "ListNode": ...
    @overload
    def __getitem__(self, key: Union[str, int, "Node"]) -> Any: ...
    def __getitem__(self, key: Union[slice, str, int, "Node"]) -> Any:
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
        >>> node = dag.put({"a": 1, "b": [5, 6]})
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
        return make_node(self.dag, self.dag.dml.get(self, key))

    def contains(self, item, *, name=None, doc=None) -> "ScalarNode":
        """
        For collection nodes, checks to see if `item` is in `self`

        Returns
        -------
        Node
            Node with the boolean of is `item` in `self`
        """
        return make_node(self.dag, self.dag.dml.contains(self, item, name=name, doc=doc))

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
        return self._info["length"]


class ListNode(CollectionNode):  # noqa: F811
    """
    Representation of a collection node in a DaggerML DAG.

    Parameters
    ----------
    dag : Dag
        Parent DAG
    ref : Ref
        Node reference
    """

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
        for i in range(len(self)):
            yield self[i]

    def conj(self, item, *, name=None, doc=None) -> "ListNode":
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
        return make_node(self.dag, self.dag.dml.conj(self, item, name=name, doc=doc))

    def append(self, item, *, name=None, doc=None) -> "ListNode":
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


class DictNode(CollectionNode):  # noqa: F811
    def keys(self) -> list[str]:
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
        list[str]
            List of keys in the dictionary node
        """
        return self._info["keys"].copy()

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
        for k in self.keys():
            yield k

    def get(self, key, default=None, *, name=None, doc=None) -> "Node":
        """
        For a dict node, return the value for key if key exists, else default.

        If default is not given, it defaults to None, so that this method never raises a KeyError.
        """
        return make_node(self.dag, self.dag.dml.get(self, key, default, name=name, doc=doc))

    def items(self) -> Iterator[tuple[str, "Node"]]:
        """
        Iterate over key-value pairs of a dictionary node.

        Returns
        -------
        Iterator[tuple[Node, Node]]
            Iterator over (key, value) pairs
        """
        if self.type != "dict":
            raise Error(f"Cannot iterate items of type: {self.type}", origin="dml", type="TypeError")
        for k in self:
            yield k, self[k]

    def values(self) -> list["Node"]:
        """
        Get the values of a dictionary node.

        Parameters
        ----------
        name : str, optional
            Name for the result node
        doc : str, optional
            Documentation

        Returns
        -------
        list[Node]
            List of values in the dictionary node
        """
        return [self[k] for k in self]

    def assoc(self, key, value, *, name=None, doc=None) -> "DictNode":
        """
        For a dict node, associate a new value into the map

        Returns
        -------
        Node
            Node containing the new dict
        """
        return make_node(self.dag, self.dag.dml.assoc(self, key, value, name=name, doc=doc))

    def update(self, update) -> "DictNode":
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
