import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Any, Iterator, Optional, Union, cast, overload

from daggerml._internal import (
    Dml,
    DmlRepoError,
    Error,
    Ref,
    Runnable,
    Uri,
)
from daggerml.codecs import CodecError, stage_value
from daggerml.util import BackoffWithJitter, current_time_millis

log = logging.getLogger(__name__)


Scalar = Union[str, int, float, bool, type(None), Uri, Runnable]
Collection = Union[list, tuple, dict]
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


def new(name="", *, message="", argv_ptr=None, dml: Dml | None = None) -> "Dag":
    """Create a new DAG using the active or provided Dml runtime."""
    runtime = dml or get_default_dml()
    index_id = runtime.runtime.create(argv_ptr=argv_ptr)
    return Dag(dml=runtime, token=index_id, name=name, message=message)


def load(name: str, dml=None) -> "Dag":
    """Load a DAG using the active default Dml runtime."""
    dml = dml or get_default_dml()
    dag_info = dml.dag.get(name)
    if dag_info is None:
        raise DmlRepoError(f"DAG not found: {name}")
    return Dag(dml=dml, ref=dag_info["dag"]["ref"], name=name)


@contextmanager
def temporary(**kw):
    """Create a temporary Dml runtime with an initial commit."""
    kw["name"] = kw.get("name", "temp")
    with TemporaryDirectory() as tmpdir:
        resp = Dml.init(project_home=tmpdir, **kw)
        yield Dml(resp["project_home"], remote_uri=resp["remote_uri"])


def status() -> dict[str, object]:
    """Return status for the active default Dml runtime."""
    dml, source = _resolve_default_dml(create=True)
    return {
        "default": {
            "source": source,
            "has_scoped_override": _SCOPED_DEFAULT_DML.get() is not _NO_DEFAULT_DML,
            "has_process_default": _PROCESS_DEFAULT_DML is not None,
        },
        "status": dml.status(),
    }


def _make_node(dag: "Dag", ref: Ref) -> "Node":
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
    node_value = dag.dml.dag.get_node(ref)["node"]
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
        return self.dml.runtime.put_literal(index_id, value, name=name)

    def _stage_value(self, value: Any, *, name: Optional[str] = None) -> Ref:
        try:
            return stage_value(self, value, name=name)
        except CodecError as e:
            raise DmlRepoError(str(e)) from e

    def _start_fn(
        self, argv: list[Ref], *, kwargv: Optional[dict[str, Ref]] = None, name: Optional[str] = None
    ) -> Optional[Ref]:
        return self.dml.runtime.start_fn(self._require_index_ref(), argv, kwargv=kwargv, name=name)

    def _call_builtin(self, uri: str, *args: Any, name: Optional[str] = None, default: Any = None) -> Ref:
        fn_ref = self._put_literal(Runnable(target=Uri(uri), kwargs={}, adapter=""))
        argv: list[Ref] = [fn_ref]
        for arg in args:
            argv.append(arg if isinstance(arg, Ref) else self._stage_value(arg))
        if default is not None:
            argv.append(default if isinstance(default, Ref) else self._stage_value(default))
        result = self._start_fn(argv, name=name)
        if result is None:
            raise DmlRepoError("Function execution failed")
        return result

    def __len__(self) -> int:
        if self.ref is None:
            info = self.dml.runtime.describe(self._require_index_ref())
            return len(info["names"])
        names_dict = self.dml.dag.describe(cast(Ref, self.ref))["dag"]["names"]
        return len(names_dict)

    def __iter__(self):
        yield from self.keys()

    def _get_named_node(self, name: str) -> "Node":
        if self.ref is None:
            node_ref = self.dml.runtime.get_node(self._require_index_ref(), name)
            return _make_node(self, node_ref)
        node_ref = self.dml.dag.describe_node(name, dag_selector=cast(Ref, self.ref))["node"]["ref"]
        return _make_node(self, node_ref)

    def _set_named_node(self, name: str, value: Any) -> None:
        if self.ref is not None:
            raise DmlRepoError("Cannot set node names on a committed DAG.")
        if isinstance(value, Ref):
            self.dml.runtime.set_node_name(self._require_index_ref(), name, value)
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
            info = self.dml.runtime.describe(self._require_index_ref())
            return list(info["names"].keys())
        names_dict = self.dml.dag.describe(cast(Ref, self.ref))["dag"]["names"]
        return list(names_dict.keys())

    def values(self) -> list["Node"]:
        """Get the list of all nodes in the dag"""
        if self.ref is None:
            info = self.dml.runtime.describe(self._require_index_ref())
            return [_make_node(self, ref) for ref in info["names"].values()]
        names_dict = self.dml.dag.describe(cast(Ref, self.ref))["dag"]["names"]
        return [_make_node(self, ref) for ref in names_dict.values()]

    @property
    def argv(self) -> "ListNode":
        "Access the dag's argv node"
        if self.ref is None:
            argv_ref = self.dml.runtime.get_argv(self._require_index_ref())
            return cast(ListNode, _make_node(self, argv_ref))
        argv_ref = self.dml.dag.describe(cast(Ref, self.ref))["dag"]["argv"]
        assert isinstance(argv_ref, Ref), f"'{self.__class__.__name__}' dag has no argv"
        return cast(ListNode, _make_node(self, argv_ref))

    @property
    def result(self) -> "Node":
        """Get the result node of the dag"""
        ref = self.dml.dag.describe(cast(Ref, self.ref))["dag"].get("result")
        assert isinstance(ref, Ref), f"'{self.__class__.__name__}' dag has not been committed yet"
        return _make_node(self, ref)

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
        >>> import daggerml as _dml
        >>> dml = _dml.temporary()
        >>> dag = new(dml=dml, name="test", message="test")
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
        return _make_node(self, self._stage_value(value, name=name))

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
        kwargv_refs: dict[str, Ref] = {}
        for key, value in kw.items():
            kwargv_refs[key] = self._stage_value(value)

        sleep = sleep or BackoffWithJitter()
        argv_seed = [fn, *args]
        end = current_time_millis() + timeout
        while timeout <= 0 or current_time_millis() < end:
            argv_refs = [self._stage_value(value) for value in argv_seed]
            resp = self._start_fn(argv_refs, kwargv=kwargv_refs, name=name)
            if resp:
                return _make_node(self, resp)
            time.sleep(sleep() / 1000)
        raise TimeoutError(f"invoking function: {fn}")

    def commit(self, value) -> None:
        """
        Commit a value to the DAG.

        Parameters
        ----------
        value : Union[Node, Error, Any]
            Value to commit
        """
        branch = self.dml.branch()["head"]
        if branch is None:
            raise DmlRepoError("Current checkout is detached; attach HEAD to commit")

        # For Errors, pass directly to _commit (don't try to store as literal)
        if isinstance(value, Error):
            commit_ref = self.dml.runtime.commit(
                self._require_index_ref(),
                value,
                head=branch,
                message=self.message,
                dag_name=self.name,
            )
        else:
            # For other values, ensure it's a Node and get its ref
            value = value if isinstance(value, Node) else self.put(value)
            value_ref = value.ref
            commit_ref = self.dml.runtime.commit(
                self._require_index_ref(),
                value_ref,
                head=branch,
                message=self.message,
                dag_name=self.name,
            )

        # Extract the dag ref from the commit
        self.ref = self.dml.dag.list(revision=commit_ref.to)["dags"][self.name]


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
        node_info = self.dag.dml.dag.describe_node(self.ref)["node"]
        argv = node_info.get("argv")
        if argv is None:
            raise Error("Node has no argv", origin="dml", type="TypeError")
        return _make_node(self.dag, argv)

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
        >>> import daggerml as _dml
        >>> dml = _dml.temporary()
        >>> dag = new(dml=dml, name="test", message="test")
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
        Load this node's execution context (DAG).

        Returns
        -------
        Dag
            This node's execution dag.
        """
        node_info = self.dag.dml.dag.describe_node(self.ref)["node"]
        dag_ref = node_info.get("dag")
        if isinstance(dag_ref, Ref):
            return Dag(dml=self.dag.dml, ref=dag_ref)
        return self.dag

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
        return self.dag.dml.dag.unroll_node(self.ref)["node"]

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
        return cast(ScalarNode, _make_node(self.dag, result))

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
        return _make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key))

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
        return cast(ListNode, _make_node(self.dag, resp))

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
        return _make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key))

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
        return _make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key, name=name, default=default))

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
        return cast(DictNode, _make_node(self.dag, resp))

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


def codecs() -> list[Any]:
    from daggerml.codecs import codecs as builtins

    return builtins()
