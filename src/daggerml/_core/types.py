"""Data model and type definitions for the DML repository system.

Contains all data classes, type aliases, constants, and helper functions
without any repository logic or LMDB dependencies.

Public API:
    Data classes - Datum, Error, Dag, Node types, Commit, Tree
    Constants - NONE
    Type aliases - Scalar
    Exception - DmlRepoError
    Functions - require_ref


from getpass import getuser
"""

import logging
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Optional, Self, TypeAlias, Union, cast, dataclass_transform
from uuid import uuid4

from daggerml._core.db import DmlDb as RawDmlDB
from daggerml._core.db import DmlDbKeyNotFoundError, Ref
from daggerml._core.util import now

logger = logging.getLogger(__name__)

# Type alias for scalar data
Scalar = Optional[Union[int, float, str, bool]]

# Constants
NONE = uuid4()

# Registries for object namespaces and node types
NAMESPACES: dict[str, type] = {}


@dataclass
class CommitCtx:
    """Context helper for accessing commit/tree/dag state without pointers."""

    commit: "Commit"
    tree: "Tree"
    dag: "Dag | None" = None


def require_ref(
    ref: Any,
    expected_ns: Optional[list[str]] = None,
    context: Optional[str] = None,
) -> None:
    """Validate that a value is a Ref and (optionally) in an expected namespace hierarchy.

    The expected_ns is a list of strings representing the namespace hierarchy to match.
    The ref's namespace hierarchy (from ref.nss()) must start with expected_ns.

    Parameters
    ----------
    ref : Any
        The value to check.
    expected_ns : list[str] | None
        Expected namespace hierarchy as a list (e.g., ["node"] or ["node", "argv"]).
        If None, only the Ref type is validated.
    context : str | None
        Optional context to include in error messages (typically the
        offending `Class.property`).

    Raises
    ------
    TypeError
        If validation fails.
    """
    ctx = f"{context}: " if context else ""
    if not isinstance(ref, Ref):
        raise TypeError(f"{ctx}expected Ref, got value {ref!r} of type {type(ref).__name__}")
    if expected_ns is not None:
        hierarchy = ref.nss()
        ns = ref.ns()
        if hierarchy[: len(expected_ns)] != expected_ns:
            raise TypeError(f"{ctx}expected namespace hierarchy {expected_ns}, got {hierarchy} for {ref!r}")
        if ns not in NAMESPACES:
            raise TypeError(f"{ctx}namespace {ns} not registered for {ref!r}")


@dataclass_transform()
def _register_dml_obj(cls) -> type:
    """Decorator to register dataclass with namespace.

    Registers the class in NAMESPACES using lowercase class name,
    and applies dataclass decorator.

    Parameters
    ----------
    cls : type
        The class to register.

    Returns
    -------
    type
        The dataclass-decorated class with _ns attribute.
    """
    namespace = cls.__name__.lower()
    if namespace not in NAMESPACES:
        NAMESPACES[namespace] = cls
    obj = dataclass(cls)
    obj._ns = namespace
    return obj


@dataclass
class DmlBase:
    """Base class for DML data objects with serialization support."""

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding private attributes.

        Returns
        -------
        dict
            Dictionary representation excluding keys starting with '_'.
        """
        return {f.name: getattr(self, f.name) for f in fields(self) if not f.name.startswith("_")}

    @classmethod
    def from_dict(cls, d: dict) -> Self:
        """Create instance from dictionary.

        Parameters
        ----------
        d : dict
            Dictionary with field data.

        Returns
        -------
        Self
            Instance created from dictionary data.
        """
        return cls(**d)

    def _validate(self) -> None:
        """Default no-op validation for DML objects.

        Subclasses should override to implement strict validation of
        field types and expected Ref namespaces.
        """
        return


class Datum(DmlBase):
    """Base class for data values in the DML system.

    Datum subclasses represent different types of data: scalars, lists, dicts,
    URIs, and runnables. Each subclass is registered under its own
    `datum-<typename>` namespace.

    Notes
    -----
    - Datum *itself* is abstract and is NOT registered as a top-level
      namespace. Each concrete Datum subclass is registered under its own
      `datum-<typename>` namespace and gets its `._ns` attribute set so it
      can be stored directly without an explicit `to=` Ref.
    """

    def __init_subclass__(cls, **kwargs):
        """Register datum subclasses for deserialization and DB namespace.

        For a subclass `ScalarDatum` we register:
          - NAMESPACES['datum-scalar'] = ScalarDatum (for LMDB namespace lookups)
          - ScalarDatum._ns = 'datum-scalar'        (so instances can be put without `to=`)
        """
        super().__init_subclass__(**kwargs)
        name = getattr(cls, "__datum_name__", cls.__name__).lower()
        if name.endswith("datum"):
            name = name[:-5]
        # register concrete per-datum namespace, e.g. datum-scalar
        concrete_ns = f"datum-{name}"
        if concrete_ns not in NAMESPACES:
            NAMESPACES[concrete_ns] = cls
        # ensure instances have a concrete _ns so BaseOps.put works without `to=`
        cls._ns = concrete_ns

    def value(self, txn: "TxnWithValid") -> Any:
        """Resolve the value of this datum, recursively resolving references.

        Parameters
        ----------
        txn : "TxnWithValid"
            Transaction context to resolve references.

        Returns
        -------
        Any
            The resolved value of this datum. NOT unrolled. Just one layer.
        """
        raise NotImplementedError("Subclasses must implement value method")

    def unroll(self, txn):
        """Recursively unroll this datum to a pure Python value.

        Resolves all nested references and datums to produce a final
        Python value (e.g., int, list, dict).

        Parameters
        ----------
        txn : "TxnWithValid"
            Transaction context to resolve references.

        Returns
        -------
        Any
            The fully unrolled Python value represented by this datum.
        """

        val = self.value(txn)
        if isinstance(val, list):
            return [txn.get(v).unroll(txn) for v in val]
        if isinstance(val, dict):
            return {k: txn.get(v).unroll(txn) for k, v in val.items()}
        return val


@dataclass
class ScalarDatum(Datum):
    """Datum containing a scalar value.

    Attributes
    ----------
    data : Scalar
        The scalar value (None, int, float, str, or bool).
    """

    data: Scalar

    def _validate(self) -> None:
        if not isinstance(self.data, (type(None), int, float, str, bool)):
            raise TypeError(
                f"{self.__class__.__name__}.data must be a scalar "
                f"(None, int, float, str, bool), got: {type(self.data).__name__}"
            )

    def value(self, txn: "TxnWithValid") -> Scalar:
        return self.data


@dataclass
class ListDatum(Datum):
    """Datum containing a list of references to other datums.

    Attributes
    ----------
    data : list[Ref]
        List of references to datum objects.
    """

    data: list[Ref]  # -> datum

    def _validate(self) -> None:
        if not isinstance(self.data, list):
            raise TypeError(f"{self.__class__.__name__}.data must be a list")
        for i, item in enumerate(self.data):
            if not isinstance(item, Ref):
                raise TypeError(f"{self.__class__.__name__}.data[{i}] must be a Ref, got {type(item).__name__}")
            if not item.nss()[0] == "datum":
                raise TypeError(f"{self.__class__.__name__}.data[{i}] must be a Ref to datum-*, got {item.ns()}")

    def value(self, txn: "TxnWithValid") -> list[Ref]:
        return self.data


@dataclass
class DictDatum(Datum):
    """Datum containing a dictionary of references to other datums.

    Attributes
    ----------
    data : dict[str, Ref]
        Dictionary mapping strings to references to datum objects.
    """

    data: dict[str, Ref]  # -> datum

    def _validate(self) -> None:
        if not isinstance(self.data, dict):
            raise TypeError(f"{self.__class__.__name__}.data must be a dict")
        for k, v in self.data.items():
            if not isinstance(k, str):
                raise TypeError(f"{self.__class__.__name__}.data keys must be strings, got {type(k).__name__}")
            if not isinstance(v, Ref):
                raise TypeError(f"{self.__class__.__name__}.data[{k!r}] must be a Ref, got {type(v).__name__}")
            if not v.nss()[0] == "datum":
                raise TypeError(f"{self.__class__.__name__}.data[{k!r}] must be a Ref to datum-*, got {v.ns()}")

    def value(self, txn: "TxnWithValid") -> dict[str, Any]:
        return self.data


@dataclass
class Uri:
    uri: str


@dataclass
class UriDatum(Datum):
    uri: str

    def _validate(self) -> None:
        if not isinstance(self.uri, str):
            raise TypeError(f"{self.__class__.__name__}.uri must be a string, got: {self.uri!r}")

    def value(self, txn: "TxnWithValid") -> Uri:
        return Uri(self.uri)


@dataclass
class Runnable:
    target: Uri
    sub: Optional["Runnable"] = None
    kwargs: dict[str, Any] = field(default_factory=dict)
    adapter: str = ""

    def innermost(self) -> "Runnable":
        """Get the innermost Runnable in the chain."""
        current = self
        while current.sub is not None:
            current = current.sub
        return current


@dataclass
class RunnableDatum(Datum):
    target: Ref  # -> datum-uri
    sub: Optional[Ref]  # -> datum-runnable
    kwargs: Ref  # -> datum-dict
    adapter: str

    def _validate(self) -> None:
        tname = self.__class__.__name__
        if not isinstance(self.target, Ref):
            raise TypeError(f"{tname}.target must be a Ref, got {type(self.target).__name__}")
        if self.target.ns() != "datum-uri":
            raise TypeError(f"{tname}.target must be a Ref to datum-uri, got {self.target.ns()}")
        if self.sub is not None:
            if not isinstance(self.sub, Ref):
                raise TypeError(f"{tname}.sub must be a Ref or None, got {type(self.sub).__name__}")
            if self.sub.ns() != "datum-runnable":
                raise TypeError(f"{tname}.sub must be a Ref to datum-runnable, got {self.sub.ns()}")
        if not isinstance(self.kwargs, Ref):
            raise TypeError(f"{tname}.kwargs must be a Ref, got {type(self.kwargs).__name__}")
        if self.kwargs.ns() != "datum-dict":
            raise TypeError(f"{tname}.kwargs must be a Ref to datum-dict, got {self.kwargs.ns()}")
        if not isinstance(self.adapter, str):
            raise TypeError(f"{tname}.adapter must be a string, got: {self.adapter!r}")

    def value(self, txn: "TxnWithValid") -> Runnable:
        target_uri = txn.get(self.target).unroll(txn)
        sub_runnable = txn.get(self.sub).unroll(txn) if self.sub is not None else None
        kwargs_dict = txn.get(self.kwargs).unroll(txn)
        return Runnable(target=target_uri, sub=sub_runnable, kwargs=kwargs_dict, adapter=self.adapter)


@_register_dml_obj
class Deletable(DmlBase):
    """URI-backed value marked for deletion during garbage collection.

    Signals that this object can be deleted during garbage collection operations.
    """

    uri: str

    def _validate(self) -> None:
        if not isinstance(self.uri, str):
            raise TypeError(f"{self.__class__.__name__}.uri must be a string, got: {self.uri!r}")

    @classmethod
    def from_uri(cls, uri_datum: Uri) -> Self:
        """Create deletable from a Uri datum.

        Parameters
        ----------
        uri_datum : Uri
            The Uri datum to convert.
        Returns
        -------
        Deletable
            A deletable for the given URI.
        """
        return cls(uri_datum.uri)


@_register_dml_obj
class Error(DmlBase, Exception):
    """Error information with stack traces.

    Represents a captured error from a computation, storing error details
    and stack trace information for debugging.

    Attributes
    ----------
    message : str
        The error message.
    origin : str
        The origin/source of the error (e.g., 'python', 'adapter').
    type : str
        The error type name.
    stack : list[dict]
        Stack trace frames as dictionaries.
    """

    message: str
    origin: str
    type: str
    stack: list[dict] = field(default_factory=list)

    def __post_init__(self):
        """Initialize Exception base with message and run base initialization."""
        Exception.__init__(self, self.message)

    def _validate(self) -> None:
        if not isinstance(self.message, str):
            raise TypeError(f"{self.__class__.__name__}.message must be a string")
        if not isinstance(self.origin, str):
            raise TypeError(f"{self.__class__.__name__}.origin must be a string")
        if not isinstance(self.type, str):
            raise TypeError(f"{self.__class__.__name__}.type must be a string")
        if not isinstance(self.stack, list):
            raise TypeError(f"{self.__class__.__name__}.stack must be a list of frame dicts")
        for frame in self.stack:
            if not isinstance(frame, dict):
                raise TypeError(f"{self.__class__.__name__}.stack frame must be a dict")

    @classmethod
    def from_ex(cls, exc) -> "Error":
        """Create Error from Python exception.

        Parameters
        ----------
        exc : Exception
            Python exception to convert.

        Returns
        -------
        Error
            Error object with extracted stack trace.
        """
        if isinstance(exc, Error):
            return Error(message=exc.message, origin=exc.origin, type=exc.type, stack=list(exc.stack))
        tb = traceback.extract_tb(exc.__traceback__)
        stack = [
            {
                "filename": frame.filename,
                "lineno": frame.lineno,
                "name": frame.name,
                "line": frame.line,
            }
            for frame in tb
        ]
        return cls(
            message=str(exc),
            origin="python",
            type=type(exc).__name__.lower(),
            stack=stack,
        )


class DmlRepoError(Error):
    """Exception raised by DML repository operations."""

    def __init__(
        self,
        message: str,
        *,
        origin: str = "dml",
        type: str = "dmlrepoerror",
        stack: Optional[list[dict]] = None,
    ):
        super().__init__(message=message, origin=origin, type=type, stack=stack or [])


class BadExecutionStatusError(DmlRepoError):
    """Raised when an execution lifecycle cannot satisfy a requested mutation mode."""

    def __init__(self, message: str, *, lifecycle: str | None = None):
        super().__init__(message, type="badexecutionstatuserror")
        self.lifecycle = lifecycle


class CanceledExecutionError(BadExecutionStatusError):
    """Raised when cancellation lifecycle blocks activation or mutation."""

    def __init__(self, message: str, *, lifecycle: str | None = None):
        super().__init__(message, lifecycle=lifecycle)
        self.type = "canceledexecutionerror"


class DmlPointerConflictError(DmlRepoError):
    """Raised when a branch or index commit update loses a stale-write race."""

    def __init__(self, message: str, *, current_commit: Ref):
        super().__init__(message, type="dmlpointerconflicterror")
        self.current_commit = current_commit


GetDatumRefType: TypeAlias = tuple[Ref, None] | tuple[None, Ref]


class Node(DmlBase):
    """Base class for computational nodes in a DAG.

    Nodes represent individual computation steps or values in a directed
    acyclic graph. They can be literal values, imports from other DAGs,
    or function calls.

    Notes
    -----
    - Node *itself* is abstract and is NOT registered as a top-level
      namespace. Each concrete Node subclass is registered under its own
      `node-<typename>` namespace and gets its `._ns` attribute set so it
      can be stored directly without an explicit `to=` Ref.
    """

    def __init_subclass__(cls, **kwargs):
        """Register node subclasses for deserialization and DB namespace.

        For a subclass `ArgvNode` we register:
          - NAMESPACES['node-argv'] = ArgvNode (for LMDB namespace lookups)
          - ArgvNode._ns = 'node-argv'        (so instances can be put without `to=`)
        """
        super().__init_subclass__(**kwargs)
        name = cls.__name__.lower()
        if name.endswith("node"):
            name = name[:-4]
        # register concrete per-node namespace, e.g. node-argv
        concrete_ns = f"node-{name}"
        if concrete_ns not in NAMESPACES:
            NAMESPACES[concrete_ns] = cls
        # ensure instances have a concrete _ns so BaseOps.put works without `to=`
        cls._ns = concrete_ns

    def datum_ref(self, txn: "TxnWithValid") -> GetDatumRefType:
        raise NotImplementedError("Subclasses must implement datum_ref method")


@dataclass
class LiteralNode(Node):
    """Node containing a literal value or error.

    Attributes
    ----------
    value : Ref
        Reference to a Datum or Error object.
    """

    value: Ref  # => datum

    def _validate(self) -> None:
        require_ref(self.value, expected_ns=["datum"], context=f"{self.__class__.__name__}.value")

    def datum_ref(self, txn: "TxnWithValid") -> GetDatumRefType:
        """Get the Datum reference for this node's value.

        Parameters
        ----------
        txn : "TxnWithValid"
            Transaction context to resolve references.

        Returns
        -------
        Ref
            The Datum reference for this node's value.
        """
        return self.value, None


@dataclass
class ArgvNode(LiteralNode):
    def cache_key(self, txn: "TxnWithValid") -> str:
        datum_ref, error_ref = self.datum_ref(txn)
        if error_ref is not None:
            raise txn.get(error_ref)
        assert datum_ref is not None
        return datum_ref.id()


@dataclass
class ImportNode(Node):
    """Node importing a result from another DAG.

    Attributes
    ----------
    dag : Ref
        Reference to the source DAG.
    node : Ref
        Reference to the specific node in that DAG.
    """

    dag: Ref  # => dag
    node: Ref  # => node

    def _validate(self) -> None:
        require_ref(self.dag, expected_ns=["dag"], context=f"{self.__class__.__name__}.dag")
        require_ref(self.node, expected_ns=["node"], context=f"{self.__class__.__name__}.node")

    def datum_ref(self, txn: "TxnWithValid") -> GetDatumRefType:
        """Get the value from the imported node.

        Returns
        -------
        Ref
            The value reference from the imported node.

        Raises
        ------
        DmlRepoError
            If node not associated with a transaction context.
        """
        node = txn.get(self.node)
        return node.datum_ref(txn)


@dataclass
class FnNode(Node):
    """Node representing a function call with arguments.

    Attributes
    ----------
    dag : Ref
        Reference to the function's DAG.
    node : Ref
        Reference to the result node.
    argv : list[Ref]
        List of argument node references.
    """

    argv: list[Ref]  # => node
    dag: Ref  # => dag

    def _validate(self) -> None:
        require_ref(self.dag, expected_ns=["dag"], context=f"{self.__class__.__name__}.dag")
        if not isinstance(self.argv, list):
            raise TypeError("argv must be a list of node Refs")
        for a in self.argv:
            require_ref(a, expected_ns=["node"], context=f"{self.__class__.__name__}.argv")

    def datum_ref(self, txn: "TxnWithValid") -> GetDatumRefType:
        """Get the value from the function call node.

        Returns
        -------
        Ref
            The value reference from the function call node.

        Raises
        ------
        DmlRepoError
            If node not associated with a transaction context.
        """
        dag = txn.get(self.dag)
        if dag.error is not None:
            return None, dag.error
        if dag.result is None:
            raise DmlRepoError("DAG has no result node")
        node = txn.get(dag.result)
        return node.datum_ref(txn)


@_register_dml_obj
class Dag(DmlBase):
    nodes: list[Ref]  # -> node
    names: dict[str, Ref]  # -> node
    result: Optional[Ref] = None  # -> node
    error: Optional[Ref] = None  # -> error
    argv: Optional[Ref] = None  # -> node-argv

    def _validate(self) -> None:
        tname = self.__class__.__name__
        if not isinstance(self.nodes, list):
            raise TypeError(f"{tname}.nodes must be a list of Refs")
        for n in self.nodes:
            require_ref(n, expected_ns=["node"], context=f"{tname}.nodes")
        if not isinstance(self.names, dict):
            raise TypeError(f"{tname}.names must be a dict of str->Ref")
        for k, v in self.names.items():
            if not isinstance(k, str):
                raise TypeError(f"{tname}.names keys must be strings")
            require_ref(v, expected_ns=["node"], context=f"{tname}.names[{k!r}]")
        if self.result is not None and self.error is not None:
            raise TypeError(f"{tname}: cannot have both result and error")
        if self.result is not None:
            require_ref(self.result, expected_ns=["node"], context=f"{tname}.result")
        if self.error is not None:
            require_ref(self.error, expected_ns=["error"], context=f"{tname}.error")
        if self.argv is not None:
            require_ref(self.argv, expected_ns=["node", "argv"], context=f"{tname}.argv")

    def nameof(self, ref):
        """Get the name of a node reference.

        Parameters
        ----------
        ref : Ref
            The node reference to look up.

        Returns
        -------
        str | None
            The name of the node, or None if not named.
        """
        return {v: k for k, v in self.names.items()}.get(ref)

    def is_finished(self, success: Optional[bool] = None) -> bool:
        """Check if the DAG has a result node.

        Parameters
        ----------
        success : bool | None
            If True, check for successful result (no error).
            If False, check for error result.
            If None, check for either result or error.

        Returns
        -------
        bool
            True if the DAG has a result, False otherwise.
        """
        if success is True:
            return self.result is not None
        if success is False:
            return self.error is not None
        return (self.result or self.error) is not None

    def cache_key(self, txn: "TxnWithValid") -> str:
        """Compute a cache key for this DAG.

        Items in the cache are stored under `{key}`.

        Parameters
        ----------
        txn : "TxnWithValid"
            Transaction context to resolve references.

        Returns
        -------
        str
            The datum_ref.id() of the argv Datum.
        """
        if self.argv is None:
            raise DmlRepoError("Cannot compute cache key for DAG without argv.")
        argv_node = txn.get(self.argv)
        datum_ref, error_ref = argv_node.datum_ref(txn)
        if error_ref is not None:
            raise txn.get(error_ref)
        assert datum_ref is not None
        return datum_ref.id()


@_register_dml_obj
class Tree(DmlBase):
    """Named collection of DAGs and their opaque tags.

    A tree organizes multiple DAGs by name, typically representing
    different computations or workflow branches.

    Attributes
    ----------
    dags : dict[str, Ref]
        Mapping of names to DAG references.
    """

    dags: dict[str, Ref]  # -> dag
    tags: dict[str, list[str]]

    def _validate(self) -> None:
        if not isinstance(self.dags, dict):
            raise TypeError("dags must be a dict of str->Ref")
        for k, v in self.dags.items():
            if not isinstance(k, str):
                raise TypeError(f"{self.__class__.__name__}.dags keys must be strings")
            require_ref(v, expected_ns=["dag"], context=f"{self.__class__.__name__}.dags[{k!r}]")
        if not isinstance(self.tags, dict):
            raise TypeError("tags must be a dict of str->list[str]")
        for name, tags in self.tags.items():
            if not isinstance(name, str):
                raise TypeError(f"{self.__class__.__name__}.tags keys must be strings")
            if name not in self.dags:
                raise TypeError(f"{self.__class__.__name__}.tags[{name!r}] requires a named DAG")
            if not isinstance(tags, list):
                raise TypeError(f"{self.__class__.__name__}.tags[{name!r}] must be a list of strings")
            if not all(isinstance(tag, str) for tag in tags):
                raise TypeError(f"{self.__class__.__name__}.tags[{name!r}] must be a list of strings")


@_register_dml_obj
class Commit(DmlBase):
    """Versioned snapshot with metadata.

    A commit represents a point-in-time state of the repository,
    including the tree, authorship, and history information.

    Attributes
    ----------
    parents : list[Ref]
        Parent commit references (empty for initial commit).
    tree : Ref
        Reference to the Tree for this commit.
    author : str
        Name of the commit author.
    message : str
        Commit message describing the change.
    created : str
        ISO timestamp when commit was created.
    """

    parents: list[Ref]  # -> commit
    tree: Ref  # -> tree
    author: str
    message: str
    created: str = field(default_factory=now)

    def _validate(self) -> None:
        if not isinstance(self.parents, list):
            raise TypeError("parents must be a list of commit Refs")
        for p in self.parents:
            require_ref(p, expected_ns=["commit"], context=f"{self.__class__.__name__}.parents")
        require_ref(self.tree, expected_ns=["tree"], context=f"{self.__class__.__name__}.tree")
        if not isinstance(self.author, str):
            raise TypeError(f"{self.__class__.__name__}.author must be a string, got: {self.author!r}")
        if not isinstance(self.message, str):
            raise TypeError(f"{self.__class__.__name__}.message must be a string, got: {self.message!r}")
        if not isinstance(self.created, str):
            msg = f"{self.__class__.__name__}.created must be an ISO timestamp string, got: {self.created!r}"
            raise TypeError(msg)


@_register_dml_obj
class Index(Commit):
    """Mutable commit-under-construction for runtime staging.

    An index carries commit-shaped history state plus the mutable DAG being
    assembled before finalization.

    Attributes
    ----------
    dag : Ref
        Reference to the in-progress DAG for this index.
    """

    dag: Ref = field(kw_only=True)

    def _validate(self) -> None:
        super()._validate()
        require_ref(self.dag, expected_ns=["dag"], context=f"{self.__class__.__name__}.dag")


@_register_dml_obj
class FrozenIndex(Commit):
    """A user runtime preserved for inspection before authoring continues."""

    dag: Ref = field(kw_only=True)
    frozen_message: Optional[str] = field(default=None, kw_only=True)

    def _validate(self) -> None:
        super()._validate()
        require_ref(self.dag, expected_ns=["dag"], context=f"{self.__class__.__name__}.dag")
        if self.frozen_message is not None and not isinstance(self.frozen_message, str):
            raise TypeError(f"{self.__class__.__name__}.frozen_message must be a string or None")


def _cleanup_opposite_entry(db: "TxnWithValid", ref: Ref, *, opposite_ns: str, noun: str) -> None:
    opposite_ref = Ref(f"{opposite_ns}:{ref.id()}")
    if db.exists(opposite_ref):
        logger.warning(
            "Clearing %s entry %s to keep %s/%s mutually exclusive (new: %s)",
            noun,
            opposite_ref,
            ref.ns(),
            opposite_ns,
            ref,
        )
        db.delete(opposite_ref)


@dataclass
class TxnWithValid:
    """Context manager for DmlDB transactions with validation on commit."""

    _txn: Any  # Placeholder for the actual transaction type

    @staticmethod
    def require(ref: Ref, expected_ns: str | list[str]) -> Ref:
        expected = [expected_ns] if isinstance(expected_ns, str) else expected_ns
        require_ref(ref, expected_ns=expected)
        return ref

    def put(self, obj, *, to: Ref | None = None, ns: str | None = None, no_overwrite: bool = False) -> Ref:
        if isinstance(obj, Ref):
            raise TypeError("Cannot put a bare Ref")
        obj = Error.from_ex(obj) if isinstance(obj, Error) else obj
        if isinstance(obj, DmlBase):
            obj._validate()
            ns = getattr(obj, "_ns", None) if (ns is None and to is None) else ns
            ref = self._txn.put(obj.to_dict(), to=to, ns=ns, no_overwrite=no_overwrite)
            if isinstance(obj, UriDatum):
                _cleanup_opposite_entry(self, ref, opposite_ns="deletable", noun="uri")
            elif isinstance(obj, Deletable):
                _cleanup_opposite_entry(self, ref, opposite_ns="datum-uri", noun="deletable")
            return ref
        raise TypeError(f"Unsupported object for DmlDB.put: {type(obj).__name__}")

    def get(self, ref: Ref):
        try:
            return self._obj_from_payload(ref, self._txn.get(ref))
        except DmlDbKeyNotFoundError:
            raise DmlRepoError(f"Object not found: {ref!r}") from None

    @staticmethod
    def _obj_from_payload(ref: Ref, payload: Any):
        ns = ref.ns()
        cls = NAMESPACES.get(ns)
        if cls is None:
            raise ValueError(f"Unknown namespace: {ns}")
        return cls.from_dict(payload)

    def exists(self, ref: Ref) -> bool:
        return self._txn.exists(ref)

    def delete(self, ref: Ref) -> None:
        self._txn.delete(ref)

    def iter(self, ns: str, start_token: str | None = None) -> Iterator[tuple[Ref, Any]]:
        for ref, payload in self._txn.iter(ns, start_token=start_token):
            yield ref, self._obj_from_payload(ref, payload)

    def list_orphans(self, start_refs: list[Ref], missing_commit_refs: set[Ref] | None = None) -> list[Ref]:
        missing = sorted(missing_commit_refs or set())
        for ref in missing:
            self.require(ref, "commit")
        return self._txn.list_orphans(start_refs, missing)

    def get_ctx(self, ref: Ref) -> CommitCtx:
        dag = None
        commit: Commit
        tree: Tree
        if ref.ns() in ("index", "frozenindex"):
            commit = self.get(ref)
            dag = self.get(cast(Index | FrozenIndex, commit).dag)
            tree = self.get(commit.tree)
        else:
            commit = self.get(self.require(ref, "commit"))
            tree = self.get(commit.tree)
        return CommitCtx(commit=commit, tree=tree, dag=cast("Dag | None", dag))


class DbNotFoundError(DmlRepoError):
    """Raised when the database is not found at the specified path."""

    def __init__(self, path: str):
        super().__init__(f"Database not found at path: {path}", type="dbnotfounderror")


class DmlDB:
    """Reusable typed database facade over ``dmldb(...)``."""

    def __init__(self, path: str, map_size_headroom: int, max_map_size: int) -> None:
        self._db = RawDmlDB(
            path,
            namespaces=sorted(NAMESPACES),
            map_size_headroom=map_size_headroom,
            max_map_size=max_map_size,
        )
        self._tx = None

    def write_with_growth(self, fn: Callable[[TxnWithValid], Any], *, create_if_missing: bool = False) -> Any:
        return self._db.write_with_growth(
            lambda txn: fn(TxnWithValid(txn)), create_if_missing=create_if_missing
        )

    @contextmanager
    def tx(self, *, readonly: bool = False, create_if_missing: bool = False):
        if not (create_if_missing or Path(self._db.path).exists()):
            raise DbNotFoundError(self._db.path)
        with self._db.tx(readonly=readonly, create_if_missing=create_if_missing) as txn:
            yield TxnWithValid(txn)

    def gc(self, refs: list[Ref], missing_commit_refs: set[Ref] | None = None) -> dict[str, int]:
        def collect(txn: TxnWithValid) -> dict[str, int]:
            stats: dict[str, int] = {}
            roots = [*refs, *(ref for ref, _ in txn.iter("index")), *(ref for ref, _ in txn.iter("frozenindex"))]
            for ref in txn.list_orphans(roots, missing_commit_refs):
                txn.delete(ref)
                stats[ref.ns()] = stats.get(ref.ns(), 0) + 1
            return stats

        return self.write_with_growth(collect)

    def exists(self) -> bool:
        """Check if the database exists on disk."""
        return Path(self._db.path).exists()

    def init(self) -> Ref:
        """Initialize the database on disk if it doesn't exist."""

        def initialize(txn: TxnWithValid) -> Ref:
            tree = txn.put(Tree(dags={}, tags={}))
            return txn.put(Commit(tree=tree, parents=[], author="dml", message="Initial commit"))

        return self.write_with_growth(initialize, create_if_missing=True)

    def ensure_exists(self) -> None:
        """Create the backing database without seeding repository history."""
        self.write_with_growth(lambda _txn: None, create_if_missing=True)
