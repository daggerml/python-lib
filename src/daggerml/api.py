from __future__ import annotations

import logging
import time
import traceback
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from importlib import metadata
from tempfile import TemporaryDirectory
from threading import RLock
from typing import Any, Literal, Optional, Protocol, Union, cast, get_args, overload

from daggerml._core import CancellationError, Dml, DmlRepoError, Error, Ref, Runnable, Uri
from daggerml.util import BackoffWithJitter, current_time_millis

logger = logging.getLogger(__name__)

Scalar = Union[str, int, float, bool, type(None), Uri, Runnable]
Collection = Union[list, dict]
ProjectionStep = Union[str, int, list[int]]

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


def new(
    name="", message="", cache_key: str | None = None, execution_id: str | None = None, dml: Dml | None = None
) -> "Dag":
    """Create a new DAG using the active or provided Dml runtime."""
    runtime = dml or get_default_dml()
    index_id = runtime.runtime.create(cache_key=cache_key, execution_id=execution_id)
    return Dag(dml=runtime, token=index_id, name=name, message=message)


def load(name: str, dml=None) -> "Dag":
    """Load a DAG using the active default Dml runtime."""
    dml = dml or get_default_dml()
    dag_ref = dml.show()["dags"].get(name)
    if dag_ref is None:
        raise DmlRepoError(f"DAG not found: {name}")
    return Dag(dml=dml, ref=dag_ref, name=name)


@contextmanager
def temporary(prefix="dml-tmp-", **kw):
    """Create a temporary Dml runtime with an unborn attached HEAD."""
    with TemporaryDirectory(prefix=prefix) as tmpdir:
        yield Dml.init(project_home=tmpdir, **kw)


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
    node_value = dag.dml.dag.get_node(ref)
    info: dict[str, Any] = {"data_type": type(node_value).__name__.lower()}
    # Determine node type based on value and populate info
    if isinstance(node_value, list):
        info["length"] = len(node_value)
        node = ListNode(dag, ref, _info=info)
    elif isinstance(node_value, dict):
        info["length"] = len(node_value)
        info["keys"] = sorted(node_value.keys())
        node = DictNode(dag, ref, _info=info)
    elif isinstance(node_value, Runnable):
        node = RunnableNode(dag, ref, _info=info)
    else:
        node = ScalarNode(dag, ref, _info=info)
    return node


def _info_for_value(value: Any) -> dict[str, Any]:
    info: dict[str, Any] = {"data_type": type(value).__name__.lower()}
    if isinstance(value, list):
        info["length"] = len(value)
    elif isinstance(value, dict):
        info["length"] = len(value)
        info["keys"] = sorted(value.keys())
    return info


def _normalize_projection_step(key: ProjectionStep | slice, *, length: int | None = None) -> ProjectionStep:
    if isinstance(key, slice):
        if key.step is not None:
            raise ValueError("Slice step is not supported")
        start = key.start if key.start is not None else 0
        if key.stop is None:
            if length is None:
                raise DmlRepoError("Slice stop requires known collection length")
            stop = length
        else:
            stop = key.stop
        return [start, stop]
    return key


def _apply_projection_step(value: Any, step: ProjectionStep) -> Any:
    if isinstance(step, list):
        if len(step) != 2:
            raise DmlRepoError("Slice projection requires exactly [start, stop]")
        return value[slice(*step)]
    return value[step]


def _apply_projection_path(value: Any, path: tuple[ProjectionStep, ...]) -> Any:
    for step in path:
        value = _apply_projection_step(value, step)
    return value


def _describe_node(node: "Node") -> Mapping[str, Any]:
    return node.dag.dml.dag.describe_node(node.ref)


def _builtin_name_for_argv(dag: "Dag", argv_refs: list[Ref]) -> str | None:
    if not argv_refs:
        return None
    runnable = dag.dml.dag.get_node(argv_refs[0], recursive=True)
    if not isinstance(runnable, Runnable):
        return None
    if runnable.adapter != "":
        return None
    if not runnable.target.uri.startswith("daggerml:"):
        return None
    return runnable.target.uri.split(":", 1)[1]


def _prepend_get_path_step(path: tuple[ProjectionStep, ...], key: ProjectionStep) -> tuple[ProjectionStep, ...] | None:
    if isinstance(key, list):
        if len(key) != 2:
            return None
        if not path:
            return None
        first, *rest = path
        if not isinstance(first, int):
            return None
        return (key[0] + first, *rest)
    return (key, *path)


def _backtrack_builtin(
    node: "Node", argv_refs: list[Ref], path: tuple[ProjectionStep, ...]
) -> tuple["Node", tuple[ProjectionStep, ...]] | None:
    builtin = _builtin_name_for_argv(node.dag, argv_refs)
    if builtin is None:
        return None
    arg_nodes = [_make_node(node.dag, ref) for ref in argv_refs[1:]]
    if builtin == "get":
        if len(arg_nodes) < 2:
            return None
        key = cast(ProjectionStep, arg_nodes[1].value())
        next_path = _prepend_get_path_step(path, key)
        if next_path is None:
            return None
        return arg_nodes[0], next_path
    if builtin == "list":
        if not path:
            return None
        index, *rest = path
        if not isinstance(index, int):
            return None
        if index < 0:
            index += len(arg_nodes)
        if index < 0 or index >= len(arg_nodes):
            return None
        return arg_nodes[index], tuple(rest)
    if builtin == "dict":
        if not path:
            return None
        key, *rest = path
        if not isinstance(key, str):
            return None
        for idx in range(0, len(arg_nodes), 2):
            if idx + 1 >= len(arg_nodes):
                break
            if arg_nodes[idx].value() == key:
                return arg_nodes[idx + 1], tuple(rest)
        return None
    if builtin == "assoc":
        if len(arg_nodes) < 3 or not path:
            return None
        selected_key, *rest = path
        assoc_key = arg_nodes[1].value()
        if selected_key == assoc_key:
            return arg_nodes[2], tuple(rest)
        return arg_nodes[0], path
    if builtin == "conj":
        if len(arg_nodes) < 2 or not path:
            return None
        index, *rest = path
        if not isinstance(index, int):
            return None
        base_len = len(arg_nodes[0].value())
        if index < 0:
            index += base_len + 1
        if index == base_len:
            return arg_nodes[1], tuple(rest)
        return arg_nodes[0], path
    return None


def _nearest_context_state(
    node: "Node", path: tuple[ProjectionStep, ...]
) -> tuple[Dag, Node, tuple[ProjectionStep, ...], bool]:
    current = node
    current_path = path
    while True:
        node_info = _describe_node(current)
        node_type = node_info["type"]
        if node_type == "ImportNode":
            source_dag = Dag(dml=current.dag.dml, ref=cast(Ref, node_info["dag"]))
            source_node = _make_node(source_dag, cast(Ref, node_info["node"]))
            if current_path:
                current = source_node
                continue
            return source_dag, source_node, current_path, True
        if node_type == "FnNode":
            argv_refs = cast(list[Ref], node_info["argv"])
            source = _backtrack_builtin(current, argv_refs, current_path)
            if source is not None:
                current, current_path = source
                continue
            fn_dag = Dag(dml=current.dag.dml, ref=cast(Ref, node_info["dag"]))
            return fn_dag, fn_dag.result, current_path, True
        return current.dag, current, current_path, False


def _resolve_context(node: "Node", path: tuple[ProjectionStep, ...], *, root: bool) -> Dag:
    context_dag, next_node, next_path, can_recurse = _nearest_context_state(node, path)
    if not root:
        return context_dag
    if not can_recurse:
        return context_dag
    return _resolve_context(next_node, next_path, root=True)


@dataclass
class Dag:
    dml: Dml
    token: Optional[Ref] = None  # Working index id
    ref: Optional[Ref] = None
    name: str = ""  # DAG name for commit
    message: str = ""  # Commit message

    def __repr__(self):
        to = self.ref.to if self.ref else (self.token.to if self.token is not None else "NA")
        return f"Dag({to})"

    def __hash__(self):
        "Useful only for tests."
        return 42

    def __eq__(self, other):
        "DAG equality is based on identity, not content."
        if not isinstance(other, Dag):
            return False
        return self.ref == other.ref and self.token == other.token and self.dml == other.dml

    def __enter__(self):
        "Catch exceptions and commit an Error"
        assert not self.ref
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_value is not None:
            # Convert exception to Error and commit it
            traceback.print_exception(exc_type, exc_value, tb)
            err = Error.from_ex(exc_value) if not isinstance(exc_value, Error) else exc_value
            self.commit(err)

    def _require_index_ref(self) -> Ref:
        if self.token is None:
            raise DmlRepoError("No active index")
        return self.token

    def _put_literal(self, value: Any, *, name: Optional[str] = None) -> Ref:
        index_id = self._require_index_ref()
        value = apply_codecs(value, dag=self)
        return self.dml.runtime.put_literal(index_id, value, name=name)

    def _start_fn(self, argv: list[Ref], *, name: Optional[str] = None) -> Optional[Ref]:
        return self.dml.runtime.start_fn(self._require_index_ref(), argv, name=name)

    def _call_builtin(self, uri: str, *args: Any, name: Optional[str] = None) -> Ref:
        fn_ref = self._put_literal(Runnable(target=Uri(uri), kwargs={}, adapter=""))
        argv: list[Ref] = [fn_ref]
        for arg in args:
            argv.append(arg if isinstance(arg, Ref) else self._put_literal(arg))
        result = self._start_fn(argv, name=name)
        if result is None:
            raise DmlRepoError("Function execution failed")
        return result

    def __len__(self) -> int:
        return len(self.keys())

    def __iter__(self):
        yield from self.keys()

    def _get_named_node(self, name: str) -> "Node":
        if self.ref is None:
            node_ref = self.dml.runtime.get_node(self._require_index_ref(), name)
            return _make_node(self, node_ref)
        node_ref = self.dml.dag.describe(self.ref)["names"].get(name)
        if node_ref is None:
            raise DmlRepoError(f"Node '{name}' not found in DAG")
        return _make_node(self, node_ref)

    def _set_named_node(self, name: str, value: Any) -> None:
        if self.ref is not None:
            raise DmlRepoError("Cannot set node names on a committed DAG.")
        if isinstance(value, Node):
            value = value.ref
        if isinstance(value, Ref):
            self.dml.runtime.set_node_name(self._require_index_ref(), name, value)
            return
        self.put(value, name=name)

    def __getitem__(self, name: str) -> "Node":
        return self._get_named_node(name)

    def __setitem__(self, name: str, value: Any) -> None:
        self.put(value, name=name)

    def __getattr__(self, name: str) -> "Node":
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    def __setattr__(self, name: str, value: Any) -> None:
        dataclass_fields = getattr(type(self), "__dataclass_fields__", {})
        if name in dataclass_fields or hasattr(type(self), name):
            object.__setattr__(self, name, value)
            return
        self[name] = value

    def keys(self) -> list[str]:
        """Get the list of all node names in the dag"""
        dag = self.ref or self.dml.runtime.describe(self._require_index_ref())["dag"]
        names_dict = self.dml.dag.describe(dag)["names"]
        return sorted(names_dict.keys())

    def values(self) -> list["Node"]:
        """Get the list of all nodes in the dag"""
        dag = self.ref or self.dml.runtime.describe(self._require_index_ref())["dag"]
        names_dict = self.dml.dag.describe(dag)["names"]
        return [_make_node(self, ref) for ref in names_dict.values()]

    @property
    def argv(self) -> "ListNode":
        "Access the dag's argv node"
        dag = self.ref or self.dml.runtime.describe(self._require_index_ref())["dag"]
        argv_ref = self.dml.dag.describe(dag)["argv"]
        if not isinstance(argv_ref, Ref):
            raise DmlRepoError(f"'{self.__class__.__name__}' dag has no argv")
        return cast(ListNode, _make_node(self, argv_ref))

    @property
    def result(self) -> "Node":
        """Get the result node of the dag"""
        if self.ref is None:
            raise DmlRepoError("Cannot access result of an uncommitted DAG")
        ref = self.dml.dag.describe(self.ref).get("result")
        if not isinstance(ref, Ref):
            raise DmlRepoError(f"'{self.__class__.__name__}' dag has not been committed yet")
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
    def put(self, value: Any, *, name=None) -> "Node": ...
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
        return _make_node(self, self._put_literal(value, name=name))

    def require(self, dag_name: str, node_name: str | None = None, *, name: str | None = None) -> "Node":
        """
        Import a node from a different (committed) DAG into the current DAG.

        Parameters
        ----------
        dag_name : str
            Name of the DAG to import from
        node_name : str, optional
            Name of the node to import. If None, imports the result node of the DAG.

        Returns
        -------
        Node
            The loaded node or DAG

        Examples
        --------
        >>> dag = new(dml=dml, name="test", message="test")
        >>> n1 = dag.put(42, name="answer")
        >>> n2 = dag.put({"a": 1, "b": [n1, "23"]}, name="data")
        >>> dag.commit(n2)
        >>> dag2 = new(dml=dml, name="test2", message="test2")
        >>> imported_n2 = dag2.require("test", "data", name="imported_data")
        >>> imported_n2.value()
        {'a': 1, 'b': [42, '23']}
        """
        index = self._require_index_ref()
        commit = self.dml.runtime.describe(index)["parents"][0]
        dag = self.dml.show(revision=commit)["dags"].get(dag_name)
        if dag is None:
            raise DmlRepoError(f"DAG not found: {dag_name}")
        dag_info = self.dml.dag.describe(dag)
        node_ref = dag_info["names"].get(node_name) if node_name else dag_info.get("result")
        if node_ref is None:
            raise DmlRepoError(f"Node '{node_name}' not found in DAG '{dag_name}'")
        node_ref = self.dml.runtime.put_import(index, dag, node_ref, name=name)
        return _make_node(self, node_ref)

    def call(
        self,
        fn: Any,
        *args: Any,
        name: Optional[str] = None,
        sleep: Optional[callable] = None,
        timeout: int = -1,
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
        argv_seed = [fn, *args]
        end = current_time_millis() + timeout
        while timeout <= 0 or current_time_millis() < end:
            argv_refs = [self._put_literal(value) for value in argv_seed]
            resp = self._start_fn(argv_refs, name=name)
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
        # errors are committed as-is, everything else is a node
        if not isinstance(value, (Error, Node)):
            value = self.put(value)
        if isinstance(value, Node):
            value = value.ref
        self.ref = self.dml.runtime.commit(self._require_index_ref(), value, message=self.message, name=self.name)
        self.token = None  # Clear the working index since it's now committed

    def cancel(self, mode: Literal["full", "drive"] = "full"):
        """Cancel the DAG's execution.

        Parameters
        ----------
        mode : str, default="plan"
            Cancellation mode. "plan" cancels the execution plan, while "drive" also attempts to stop any
            currently running tasks. "drive" is more aggressive and may be necessary for long-running DAGs.
        """
        if self.token is None:
            raise DmlRepoError("Cannot cancel a committed DAG")
        logger.info(f"Cancelling execution {self.token} with mode '{mode}'")
        self.dml.runtime.cancel(self.token, mode=mode)
        self.token = None  # Clear the index ref to indicate it's no longer active
        raise CancellationError(f"DAG execution cancelled with mode '{mode}'")


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

    def context(self, *, root: bool = True) -> Dag:
        """
        Resolve the provenance context (DAG) for this node.

        This follows import/function provenance while treating builtin
        collection-construction and selection DAGs as transparent.

        Parameters
        ----------
        root : bool, default=True
            If False, return the nearest sub-DAG in which this value exists as a
            proper node across a non-builtin import/function boundary. If True,
            continue recursively until provenance no longer crosses a
            non-builtin import/function boundary and return that first rooted
            context.

        Returns
        -------
        Dag
            The nearest or rooted provenance DAG for this node.

        Examples
        --------
        >>> source = new(dml=dml, name="source", message="source")
        >>> answer = source.put(42, name="answer")
        >>> payload = source.put({"answer": answer}, name="payload")
        >>> source.commit(payload)
        >>> consumer = new(dml=dml, name="consumer", message="consumer")
        >>> imported = consumer.require("source", "payload", name="payload")
        >>> consumer.commit(imported)
        >>> loaded = load("consumer", dml=dml)
        >>> loaded.result["answer"].context() == source
        True
        """
        return _resolve_context(self, (), root=root)

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
        return self.dag.dml.dag.get_node(self.ref, recursive=True)

    def __call__(self, *args, name=None, sleep=None, timeout=-1, **kw) -> "Node":
        raise TypeError(f"Node of type '{self.type}' is not callable")


class ScalarNode(Node):
    pass


class RunnableNode(Node):
    def __call__(self, *args, name=None, sleep=None, timeout=-1) -> "Node":
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
        return self.dag.call(self, *args, name=name, sleep=sleep, timeout=timeout)


@dataclass(frozen=True)
class Projection:
    dag: Dag
    base: Node
    path: tuple[ProjectionStep, ...]
    _info: dict = field(default_factory=dict)

    def __repr__(self):
        return f"Projection({self.base!r}, path={self.path!r})"

    @classmethod
    def from_step(cls, base: Node, step: ProjectionStep) -> "Projection":
        return cls(dag=base.dag, base=base, path=(step,))

    def _extend(self, step: ProjectionStep) -> "Projection":
        return Projection(dag=self.dag, base=self.base, path=(*self.path, step))

    def value(self):
        return _apply_projection_path(self.base.value(), self.path)

    def context(self, *, root: bool = True) -> Dag:
        return _resolve_context(self.base, self.path, root=root)

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Projection of type '{self.type}' is not callable")

    @property
    def type(self):
        if "data_type" in self._info:
            return self._info["data_type"]
        return _info_for_value(self.value())["data_type"]

    def __len__(self):
        if "length" in self._info:
            return self._info["length"]
        value = self.value()
        if not isinstance(value, (list, dict)):
            raise TypeError(f"Object of type '{type(value).__name__}' has no len()")
        return len(value)

    def __iter__(self):
        value = self.value()
        if isinstance(value, list):
            for i in range(len(value)):
                yield self[i]
            return
        if isinstance(value, dict):
            yield from self.keys()
            return
        raise TypeError(f"Object of type '{type(value).__name__}' is not iterable")

    def __getitem__(self, key: ProjectionStep | slice) -> "Projection":
        value = self.value()
        if isinstance(value, dict):
            if not isinstance(key, str):
                raise TypeError(f"Dict keys must be strings but got {type(key).__name__}")
            step = cast(ProjectionStep, key)
        elif isinstance(value, list):
            if not isinstance(key, (int, slice)):
                raise TypeError(f"List indices must be integers or slices but got {type(key).__name__}")
            step = _normalize_projection_step(cast(ProjectionStep | slice, key), length=len(value))
        else:
            raise TypeError(f"Cannot project into object of type '{type(value).__name__}'")
        projected = self._extend(step)
        return Projection(dag=self.dag, base=self.base, path=projected.path, _info=_info_for_value(projected.value()))

    def keys(self) -> list[str]:
        value = self.value()
        if not isinstance(value, dict):
            raise TypeError(f"Cannot get keys of type: {type(value).__name__}")
        return sorted(value.keys())


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
    def __getitem__(self, key: Union[slice, list[int]]) -> Union["ListNode", Projection]: ...
    @overload
    def __getitem__(self, key: Union[int, "Node"]) -> Union["Node", Projection]: ...
    def __getitem__(self, key: Union[slice, list[int], int, "Node"]) -> Union["Node", Projection]:
        if self.dag.ref is not None:
            if isinstance(key, Node):
                raise TypeError("Committed list projections require concrete int or slice keys")
            step = _normalize_projection_step(key, length=len(self))
            return Projection(
                dag=self.dag,
                base=self,
                path=(step,),
                _info=_info_for_value(_apply_projection_step(self.value(), step)),
            )
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
    def __getitem__(self, key: Union[str, "Node"]) -> Union["Node", Projection]:
        if self.dag.ref is not None:
            if not isinstance(key, str):
                raise TypeError(f"Dict keys must be strings but got {type(key).__name__}")
            return Projection(dag=self.dag, base=self, path=(key,), _info=_info_for_value(self.value()[key]))
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
        return _make_node(self.dag, self.dag._call_builtin("daggerml:get", self.ref, key, default, name=name))

    def items(self) -> Iterator[tuple[str, "Node|Projection"]]:
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

    def values(self) -> list["Node|Projection"]:
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


################################################################################
################## Codec system for encoding literals in DAGs ##################
################################################################################

LITERAL_CODEC_ENTRYPOINT_GROUP = "daggerml.codecs"
_codecs: list[tuple[int, int, "LiteralCodec"]] = []
_plugins_loaded = False
_lock = RLock()


class LiteralCodec(Protocol):
    def can_encode(self, value: Any) -> bool: ...

    def encode(self, value: Any, dag: "Dag") -> Any: ...


class CodecError(Error):
    def __init__(self, message: str):
        super().__init__(message, origin="dml-codec", type="codec-error")


def _entry_points(group=LITERAL_CODEC_ENTRYPOINT_GROUP) -> list[metadata.EntryPoint]:
    points = metadata.entry_points()
    result = list(points.select(group=group))
    result.sort(key=lambda ep: (ep.name, ep.value))
    return result


def ensure_literal_codec_plugins_loaded() -> None:
    global _plugins_loaded
    if _plugins_loaded:
        return
    with _lock:
        codec_seq = 0
        if _plugins_loaded:
            return
        loaded = []
        for entry_point in _entry_points():
            try:
                registrations = entry_point.load()()
                for item in registrations:
                    priority, codec = item
                    codec_seq += 1
                    loaded.append((priority, codec_seq, codec))
            except Exception as e:
                msg = f"Literal codec plugin '{entry_point.name} ({entry_point.value})' failed: {e}"
                raise CodecError(msg) from None
        loaded.sort(key=lambda item: (-item[0], item[1]))
        _codecs.extend(loaded)
        _plugins_loaded = True


def iter_codecs() -> Iterator[LiteralCodec]:
    ensure_literal_codec_plugins_loaded()
    yield from [codec for _priority, _seq, codec in _codecs]


def apply_codec(value: Any, *, dag: Dag) -> Any:
    for codec in iter_codecs():
        try:
            if codec.can_encode(value):
                resp = codec.encode(value, dag)
                if isinstance(resp, type(value)):
                    codec_name = codec.__class__.__name__
                    msg = f"Literal codec {codec_name} encoded {value.__class__.__name__} to {resp.__class__.__name__}."
                    raise CodecError(msg)
                return resp
        except Exception as e:
            if isinstance(e, DmlRepoError):
                raise
            raise CodecError(f"Literal codec {codec.__class__.__name__} failed: {e}") from e
    raise CodecError(f"No codec found for value of type {type(value).__name__}")


def apply_codecs(value: Any, *, dag: Dag) -> Any:
    while not isinstance(value, (*get_args(Scalar), *get_args(Collection), Error, Ref)):
        value = apply_codec(value, dag=dag)
    if isinstance(value, list):
        return [apply_codecs(v, dag=dag) for v in value]
    if isinstance(value, dict):
        return {k: apply_codecs(v, dag=dag) for k, v in value.items()}
    if isinstance(value, Uri):
        return Uri(apply_codecs(value.uri, dag=dag))
    if isinstance(value, Runnable):
        target = apply_codecs(value.target, dag=dag)
        sub = apply_codecs(value.sub, dag=dag)
        kwargs = {k: apply_codecs(v, dag=dag) for k, v in value.kwargs.items()}
        return Runnable(target=target, adapter=value.adapter, kwargs=kwargs, sub=sub)
    return value


class MiscPyTypeCodec:
    def can_encode(self, value: Any) -> bool:
        return isinstance(value, Mapping) or (
            isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
        )

    def encode(self, value: Sequence | Mapping, dag: Dag) -> Any:
        if isinstance(value, Mapping):
            return {k: apply_codecs(v, dag=dag) for k, v in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [apply_codecs(v, dag=dag) for v in value]


class NodeCodec:
    def can_encode(self, value: Any) -> bool:
        return isinstance(value, Node)

    def encode(self, value: "Node", dag: Dag) -> Ref:
        assert dag.token is not None, "DAG must have a token to encode nodes"
        if value.dag.token is not None and value.dag.token == dag.token:
            return value.ref
        if value.dag.ref is None:
            raise CodecError("Cannot encode node from uncommitted DAG in a different index")
        try:
            return dag.dml.runtime.put_import(dag._require_index_ref(), value.dag.ref, node=value.ref, name=None)
        except Exception as e:
            raise CodecError(f"Failed to encode cross-dag node import: {e}") from e


def codecs() -> list:
    return [(0, NodeCodec()), (0, MiscPyTypeCodec())]
