import logging
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Any, Iterator, Optional, Union, cast, overload

from daggerml._internal import (
    DEFAULT_HEAD,
    DmlOps,
    DmlRepoError,
    Error,
    Ref,
    Runnable,
    Uri,
)
from daggerml._internal.codec import CodecContext, register_codec
from daggerml._internal.config import DmlConfig
from daggerml.util import BackoffWithJitter, current_time_millis

log = logging.getLogger(__name__)


Scalar = Union[str, int, float, bool, type(None), Uri, Runnable]
Collection = Union[list, tuple, dict]
_DEFAULT_LITERAL_CODECS_REGISTERED = False
_NO_DEFAULT_DML = object()
_SCOPED_DEFAULT_DML: ContextVar[object] = ContextVar("daggerml_scoped_default_dml", default=_NO_DEFAULT_DML)
_PROCESS_DEFAULT_DML: Optional["Dml"] = None


def _resolve_default_dml(*, create: bool = True) -> tuple["Dml", str]:
    scoped = _SCOPED_DEFAULT_DML.get()
    if scoped is not _NO_DEFAULT_DML:
        return cast("Dml", scoped), "scoped"

    global _PROCESS_DEFAULT_DML
    if _PROCESS_DEFAULT_DML is not None:
        return _PROCESS_DEFAULT_DML, "process"

    if not create:
        raise DmlRepoError("No default Dml is configured")

    _PROCESS_DEFAULT_DML = Dml()
    return _PROCESS_DEFAULT_DML, "implicit"


def get_default_dml() -> "Dml":
    """Return the active default Dml runtime."""
    dml, _source = _resolve_default_dml(create=True)
    return dml


def set_default_dml(dml: "Dml") -> None:
    """Set the process-default Dml runtime."""
    global _PROCESS_DEFAULT_DML
    _PROCESS_DEFAULT_DML = dml


def clear_default_dml() -> None:
    """Clear the process-default Dml runtime."""
    global _PROCESS_DEFAULT_DML
    _PROCESS_DEFAULT_DML = None


@contextmanager
def use_default_dml(dml: "Dml"):
    """Temporarily override the default Dml runtime for the active context."""
    token = _SCOPED_DEFAULT_DML.set(dml)
    try:
        yield dml
    finally:
        _SCOPED_DEFAULT_DML.reset(token)


def new(name="", message="", argv_ptr=None) -> "Dag":
    """Create a new DAG using the active default Dml runtime."""
    return get_default_dml().new(name=name, message=message, argv_ptr=argv_ptr)


def load(name: Union[str, "Node"]) -> "Dag":
    """Load a DAG using the active default Dml runtime."""
    return get_default_dml().load(name)


def status() -> dict[str, object]:
    """Return status for the active default Dml runtime."""
    dml, source = _resolve_default_dml(create=True)
    cfg = (
        dml._config
        or DmlConfig.resolve(
            explicit={
                "project.home": dml.repo,
                "user": dml.user,
                "project.branch": dml.branch,
            }
        )
    )
    return {
        "default": {
            "source": source,
            "has_scoped_override": _SCOPED_DEFAULT_DML.get() is not _NO_DEFAULT_DML,
            "has_process_default": _PROCESS_DEFAULT_DML is not None,
        },
        "config": cfg.to_dict(),
        "runtime": {
            "ops_initialized": dml._ops is not None,
            "branch": dml.branch,
        },
    }


@dataclass
class Dml:
    """DaggerML repository client using direct DmlOps API."""

    # Configuration
    repo: Optional[str] = None  # Repository path
    user: Optional[str] = None
    branch: Optional[str] = None

    # Internal state
    _ops: Optional[DmlOps] = field(default=None, init=False, repr=False)
    _config: Optional[DmlConfig] = field(default=None, init=False, repr=False)
    tmpdirs: dict[str, TemporaryDirectory] = field(default_factory=dict)

    def __post_init__(self):
        resolved = DmlConfig.resolve(
            explicit={
                "project.home": self.repo,
                "user": self.user,
                "project.branch": self.branch,
            }
        )
        self._config = resolved
        self.repo = resolved.project.home
        self.user = resolved.user
        self.branch = resolved.branch

    @property
    def ops(self) -> DmlOps:
        """Get or create DmlOps instance."""
        if self._ops is None:
            if not self.repo:
                raise DmlRepoError("Repository path is required")
            remote_root = self._config.remote.uri if self._config is not None else ""
            self._ops = DmlOps.open(self.repo, remote_root=remote_root)
            self._ops.__enter__()
        _ensure_default_literal_codecs(self)
        return self._ops

    @property
    def commit(self):
        return self.ops.commit()

    @property
    def head(self):
        return self.ops.head()

    @property
    def index(self):
        return self.ops.index()

    @property
    def dag(self):
        return self.ops.dag()

    @property
    def node(self):
        return self.ops.node()

    @property
    def cache(self):
        return self.ops.cache()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self._ops is not None:
                self._ops.__exit__(exc_type, exc_value, traceback)
                self._ops = None
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up temporary directories."""
        for tmpdir in self.tmpdirs.values():
            tmpdir.cleanup()
        self.tmpdirs.clear()

    @classmethod
    def temporary(cls, repo="test", user="user", branch="main") -> "Dml":
        """Create a temporary Dml instance with a temporary repository.

        Parameters
        ----------
        repo : str, default="test"
            Repository name
        user : str, default="user"
            User name for commits
        branch : str, default="main"
            Branch name
        Returns
        -------
        Dml
            Temporary Dml instance
        """
        # Create temporary directory
        tmpdir = TemporaryDirectory(prefix="dml-")
        repo_path = os.path.join(tmpdir.name, repo)

        # Create repository and initialize
        with DmlOps.create(repo_path, user=user, remote_root="") as ops:
            # DmlOps.create initializes the default head; only add a new branch when requested.
            if branch != DEFAULT_HEAD:
                head_ops = ops.head()
                head_ops.create_branch(branch, head_ops.get_branch_commit(DEFAULT_HEAD))

        return cls(repo=repo_path, user=user, branch=branch, tmpdirs={"repo": tmpdir})

    def new(self, name="", message="", argv_ptr=None) -> "Dag":
        """Create a new DAG.

        Parameters
        ----------
        name : str, optional
            DAG name
        message : str, optional
            Commit message
        argv_ptr : str, optional
            Remote manifest pointer for argv state (used by adapter executions)
        Returns
        -------
        Dag
            New DAG instance
        """
        if argv_ptr is not None:
            index_id = self.ops.index().create(argv_ptr=argv_ptr)
        else:
            index_id = self.ops.index().create(head=self.branch)

        return Dag(dml=self, token=index_id, ref=None, name=name, message=message)

    def load(self, name: Union[str, "Node"]) -> "Dag":
        """Load an existing DAG by name.

        Parameters
        ----------
        name : str or Node
            DAG name or node containing DAG name. If a Node from an import, loads
            the source DAG of that import node.
        Returns
        -------
        Dag
            Loaded DAG instance
        """
        if isinstance(name, Node):
            node_info = self.ops.node().describe(name.ref)
            if "dag" in node_info and node_info["dag"] is not None:
                return Dag(dml=self, ref=node_info["dag"])
            dag_name = self.ops.node().unroll(name.ref)
        else:
            dag_name = name

        commit_ref = self.ops.head().get_branch_commit(cast(str, self.branch))
        dag_ref = self.ops.commit().get_dag(commit_ref, str(dag_name))

        if dag_ref is None:
            raise DmlRepoError(f"DAG '{dag_name}' not found")

        return Dag(dml=self, ref=dag_ref)


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
    # Get node info from DmlOps
    node_value = dag.dml.ops.node().get(ref)
    info: dict[str, Any] = {"data_type": type(node_value).__name__.lower()}

    # Determine node type based on value and populate info
    if isinstance(node_value, list):
        info["length"] = len(node_value)
        node = ListNode(dag, ref, _info=info)
    elif isinstance(node_value, dict):
        info["length"] = len(node_value)
        info["keys"] = list(node_value.keys())
        node = DictNode(dag, ref, _info=info)
    elif isinstance(node_value, Runnable):
        node = RunnableNode(dag, ref, _info=info)
    else:
        node = ScalarNode(dag, ref, _info=info)
    return node


@dataclass
class Dag:
    dml: Dml
    token: Optional[str] = None  # Working index id
    ref: Optional[Ref] = None
    name: str = ""  # DAG name for commit
    message: str = ""  # Commit message

    def __repr__(self):
        to = self.ref.to if self.ref else (self.token if self.token is not None else "NA")
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
            # Convert exception to Error and commit it
            err = Error.from_ex(exc_value) if not isinstance(exc_value, Error) else exc_value
            self.commit(err)

    def _require_index_ref(self) -> str:
        index_id = self.token
        if index_id is None:
            raise DmlRepoError("No active index")
        return index_id

    def _put_literal(self, value: Any, *, name: Optional[str] = None) -> Ref:
        index_id = self._require_index_ref()
        return self.dml.index.put_literal(index_id, value, name=name)

    def _start_fn(
        self, argv: list[Ref], *, kwargv: Optional[dict[str, Ref]] = None, name: Optional[str] = None
    ) -> Optional[Ref]:
        return self.dml.index.start_fn(self._require_index_ref(), argv, kwargv=kwargv, name=name)

    def _call_builtin(self, uri: str, *args: Any, name: Optional[str] = None, default: Any = None) -> Ref:
        fn_ref = self._put_literal(Runnable(target=Uri(uri), kwargs={}, adapter=""))
        argv: list[Ref] = [fn_ref]
        for arg in args:
            argv.append(arg if isinstance(arg, Ref) else self._put_literal(arg))
        if default is not None:
            argv.append(default if isinstance(default, Ref) else self._put_literal(default))
        result = self._start_fn(argv, name=name)
        if result is None:
            raise DmlRepoError("Function execution failed")
        return result

    def __len__(self) -> int:
        if self.ref is None:
            info = self.dml.ops.index().describe(self._require_index_ref())
            return len(info["names"])
        names_dict = self.dml.dag.describe(self.ref)["names"]
        return len(names_dict)

    def __iter__(self):
        yield from self.keys()

    def _get_named_node(self, name: str) -> "Node":
        if self.ref is None:
            node_ref = self.dml.ops.index().get_node(self._require_index_ref(), name)
            return make_node(self, node_ref)
        return make_node(self, self.dml.dag.get_node(self.ref, name))

    def _set_named_node(self, name: str, value: Any) -> None:
        if self.ref is not None:
            raise DmlRepoError("Cannot set node names on a committed DAG.")
        if isinstance(value, Ref):
            self.dml.index.set_node_name(self._require_index_ref(), name, value)
            return
        self.put(value, name=name)

    def __getitem__(self, name: str) -> "Node":
        if not isinstance(name, str):
            raise TypeError(f"Dag node name must be str, got {type(name).__name__}")
        return self._get_named_node(name)

    def __setitem__(self, name: str, value: Any) -> None:
        if not isinstance(name, str):
            raise TypeError(f"Dag node name must be str, got {type(name).__name__}")
        self._set_named_node(name, value)

    def __getattr__(self, name: str) -> "Node":
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    def __setattr__(self, name: str, value: Any) -> None:
        dataclass_fields = getattr(type(self), "__dataclass_fields__", {})
        if name in dataclass_fields or name.startswith("_") or hasattr(type(self), name):
            object.__setattr__(self, name, value)
            return
        self[name] = value

    def keys(self) -> list[str]:
        """Get the list of all node names in the dag"""
        if self.ref is None:
            info = self.dml.ops.index().describe(self._require_index_ref())
            return list(info["names"].keys())
        names_dict = self.dml.dag.describe(self.ref)["names"]
        return list(names_dict.keys())

    def values(self) -> list["Node"]:
        """Get the list of all nodes in the dag"""
        if self.ref is None:
            info = self.dml.ops.index().describe(self._require_index_ref())
            return [make_node(self, ref) for ref in info["names"].values()]
        names_dict = self.dml.dag.describe(self.ref)["names"]
        return [make_node(self, ref) for ref in names_dict.values()]

    @property
    def argv(self) -> "ListNode":
        "Access the dag's argv node"
        if self.ref is None:
            # Inside context manager, get argv from index
            argv_ref = self.dml.ops.index().get_argv(self._require_index_ref())
            return cast(ListNode, make_node(self, argv_ref))
        return cast(ListNode, make_node(self, self.dml.dag.get_argv(self.ref)))

    @property
    def result(self) -> "Node":
        """Get the result node of the dag"""
        ref = self.dml.dag.describe(cast(Ref, self.ref)).get("result")
        assert isinstance(ref, Ref), f"'{self.__class__.__name__}' dag has not been committed yet"
        return make_node(self, ref)

    @overload
    def load(self, dag_name: str, /, key: str = "result", *, name=None) -> "Node": ...
    @overload
    def load(self, node: "Node", /, *, name=None) -> "Node": ...
    def load(self, dag_or_node: Union[str, "Node"], /, key: str = "result", *, name=None) -> "Node":
        """Load a node from a different dag into this one.

        Parameters
        ----------
        dag_or_node : str or Node
            Source dag name or source node to import.
        key : str, default="result"
            Node name to import when dag_or_node is a dag name.
        name : str, optional
            Name to assign the resulting node in this dag

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
        source = dag_or_node
        if isinstance(source, str):
            if self.ref is None and source == self.name:
                return self.result if key == "result" else self[key]
            loaded = self.dml.load(source)
            source = loaded.result if key == "result" else loaded[key]
        source_dag = cast(Ref, source.dag.ref)
        node_ref = self.dml.index.put_import(self._require_index_ref(), source_dag, node=source.ref, name=name)
        return make_node(self, node_ref)

    @overload
    def put(self, value: Union[list, "ListNode"], *, name=None) -> "ListNode": ...
    @overload
    def put(self, value: Union[dict, "DictNode"], *, name=None) -> "DictNode": ...
    @overload
    def put(self, value: Union[Runnable, "RunnableNode"], *, name=None) -> "RunnableNode": ...
    @overload
    def put(self, value: Union[Scalar, "ScalarNode"], *, name=None) -> "ScalarNode": ...
    @overload
    def put(self, value: "Node", *, name=None) -> "Node": ...
    def put(self, value: Any, *, name=None) -> "Node":
        """
        Add a value to the DAG.

        Parameters
        ----------
        value : Union[Scalar, Collection]
            Value to add
        name : str, optional
            Name for the node
        Returns
        -------
        Node
            Node representing the value

        Examples
        --------
        >>> dml = Dml.temporary()
        >>> dag = dml.new("test", "test")
        >>> n1 = dag.put(42, name="answer")
        >>> n1.value()
        42
        >>> n2 = dag.put({"a": 1, "b": [n1, "23"]})
        >>> n2.value()
        {'a': 1, 'b': [42, '23']}
        >>> n3 = dag.put({"a": 1, "b": [n1, "23"]})
        >>> n3.value()
        {'a': 1, 'b': [42, '23']}
        """
        return make_node(self, self._put_literal(value, name=name))

    def call(
        self,
        fn: Any,
        *args: Any,
        name: Optional[str] = None,
        sleep: Optional[callable] = None,
        timeout: int = -1,
        **kw,
    ) -> "Node":
        """
        Call a function node with arguments.

        Parameters
        ----------
        fn : Union[Runnable, RunnableNode]
            Function to call
        *args : Union[Node, Scalar, Collection]
            Arguments to pass to the function
        name : str, optional
            Name for the result node
        sleep : callable, optional
            A nullary function that returns sleep time in milliseconds
        timeout : int, default=-1
            Maximum time to wait in milliseconds. If <= 0, wait indefinitely.
        **kw : dict
            Keyword arguments override default values on the function specification.

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
        fn_value = fn.value() if isinstance(fn, Node) else fn

        kwargv_refs: dict[str, Ref] = {}
        for key, value in kw.items():
            kwargv_refs[key] = value.ref if isinstance(value, Node) else self.put(value).ref

        sleep = sleep or BackoffWithJitter()
        expr = [self.put(x) for x in [fn_value, *args]]
        end = current_time_millis() + timeout
        while timeout <= 0 or current_time_millis() < end:
            # Extract refs from nodes
            argv_refs = [node.ref for node in expr]
            resp = self._start_fn(argv_refs, kwargv=kwargv_refs, name=name)
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
        # For Errors, pass directly to _commit (don't try to store as literal)
        if isinstance(value, Error):
            commit_ref = self.dml.index.commit(
                self._require_index_ref(), value, head=self.dml.branch, message=self.message, dag_name=self.name
            )
        else:
            # For other values, ensure it's a Node and get its ref
            value = value if isinstance(value, Node) else self.put(value)
            value_ref = value.ref
            commit_ref = self.dml.index.commit(
                self._require_index_ref(),
                value_ref,
                head=self.dml.branch,
                message=self.message,
                dag_name=self.name,
            )

        # Extract the dag ref from the commit
        self.ref = self.dml.ops.commit().describe(commit_ref)["dag"]

    def cache(self) -> str:
        """Publish this committed DAG to the configured remote cache ref."""
        if self.ref is None:
            raise DmlRepoError("DAG must be committed before caching")
        return self.dml.cache.put(self.ref)


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

    def __eq__(self, other):
        if not isinstance(other, Node):
            return NotImplemented
        return self.ref == other.ref

    @property
    def argv(self) -> "Node":
        "Access the node's argv list"
        node_info = self.dag.dml.node.describe(self.ref)
        argv = node_info.get("argv")
        if argv is None:
            raise Error("Node has no argv", origin="dml", type="TypeError")
        return make_node(self.dag, argv)

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
        raise NotImplementedError("Node backtracking is temporarily disabled and will be reintroduced later.")

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
    def value(self: "RunnableNode") -> Runnable: ...
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
        return self.dag.dml.node.unroll(self.ref)

    def __call__(self, *args, name=None, sleep=None, timeout=-1, **kw) -> "Node":
        raise TypeError(f"Node of type '{self.type}' is not callable")


class ScalarNode(Node):
    pass


class RunnableNode(Node):
    def __call__(self, *args, name=None, sleep=None, timeout=-1, **kw) -> "Node":
        """
        Call this node as a function.

        Parameters
        ----------
        *args : Any
            Arguments to pass to the function
        name : str, optional
            Name for the result node
        sleep : callable, optional
            A nullary function that returns sleep time in milliseconds
        timeout : int, default=-1
            Maximum time to wait in milliseconds. -1 means wait forever.
        **kw : dict
            Keyword arguments override runnable defaults.

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
        return self.dag.call(self, *args, name=name, sleep=sleep, timeout=timeout, **kw)


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

    def contains(self, item, *, name=None) -> "ScalarNode":
        """
        For collection nodes, checks to see if `item` is in `self`

        Returns
        -------
        Node
            Node with the boolean of is `item` in `self`
        """
        item_ref = item.ref if isinstance(item, Node) else item
        result = self.dag._call_builtin("daggerml:contains", self.ref, item_ref, name=name)
        return cast(ScalarNode, make_node(self.dag, result))

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
            If the node isn't a collection (e.g. list or dict).
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

    @overload
    def __getitem__(self, key: Union[slice, list[int]]) -> "ListNode": ...
    @overload
    def __getitem__(self, key: Union[int, "Node"]) -> "Node": ...
    def __getitem__(self, key: Union[slice, list[int], int, "Node"]) -> "Node":
        if isinstance(key, slice):
            if key.step is not None:
                raise ValueError("Slice step is not supported")
            start = key.start if key.start is not None else 0
            stop = key.stop if key.stop is not None else len(self)
            key = [start, stop]
        return make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key))

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
            If the node isn't a collection (e.g. list or dict).
        """
        for i in range(len(self)):
            yield self[i]

    def conj(self, item, *, name=None) -> "ListNode":
        """
        For a list node, append an item

        Returns
        -------
        Node
            Node containing the new collection

        Notes
        -----
        `append` is an alias `conj`
        """
        item_ref = item.ref if isinstance(item, Node) else item
        resp = self.dag._call_builtin("daggerml:conj", self.ref, item_ref, name=name)
        return cast(ListNode, make_node(self.dag, resp))

    def append(self, item, *, name=None) -> "ListNode":
        """
        For a list node, append an item

        Returns
        -------
        Node
            Node containing the new collection

        See Also
        --------
        conj : The main implementation
        """
        return self.conj(item, name=name)


class DictNode(CollectionNode):  # noqa: F811
    def __getitem__(self, key: Union[str, "Node"]) -> "Node":
        return make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key))

    def keys(self) -> list[str]:
        """
        Get the keys of a dictionary node.

        Parameters
        ----------
        name : str, optional
            Name for the result node

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
            If the node isn't a collection (e.g. list or dict).
        """
        for k in self.keys():
            yield k

    def get(self, key, default=None, *, name=None) -> "Node":
        """
        For a dict node, return the value for key if key exists, else default.

        If default is not given, it defaults to None, so that this method never raises a KeyError.
        """
        return make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key, name=name, default=default))

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

        Returns
        -------
        list[Node]
            List of values in the dictionary node
        """
        return [self[k] for k in self]

    def assoc(self, key, value, *, name=None) -> "DictNode":
        """
        For a dict node, associate a new value into the map

        Returns
        -------
        Node
            Node containing the new dict
        """
        value_ref = value.ref if isinstance(value, Node) else value
        resp = self.dag._call_builtin("daggerml:assoc", self.ref, key, value_ref, name=name)
        return cast(DictNode, make_node(self.dag, resp))

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


@dataclass(frozen=True)
class NodeCodec:
    def can_encode(self, value: Any) -> bool:
        return isinstance(value, Node)

    def encode(self, value: Node, ctx: CodecContext) -> Ref:
        if value.dag.token is not None and value.dag.token == ctx.index_id:
            return value.ref
        if value.dag.ref is None:
            raise DmlRepoError("Cannot encode node from uncommitted DAG in a different index")
        if value.dag.ref == ctx.index_ops.current_dag_ref(ctx.index_id):
            return value.ref
        try:
            return cast(Ref, ctx.index_ops.put_import(ctx.index_id, value.dag.ref, node=value.ref, name=None))
        except Exception as e:
            raise DmlRepoError(f"Failed to encode cross-dag node import: {e}") from e


def _ensure_default_literal_codecs(dml: Dml) -> None:
    global _DEFAULT_LITERAL_CODECS_REGISTERED
    if _DEFAULT_LITERAL_CODECS_REGISTERED:
        return
    register_codec(NodeCodec(), priority=0)
    _DEFAULT_LITERAL_CODECS_REGISTERED = True
