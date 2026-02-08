"""Data model and type definitions for the DML repository system.

Contains all data classes, type aliases, constants, and helper functions
without any repository logic or LMDB dependencies.

Public API:
    Data classes - Datum, Error, Dag, Node types, Commit, Tree, Head, Index
    Constants - NONE, DEFAULT_HEAD, DEFAULT_USER
    Type aliases - Scalar, MaybeRef*, Collection types
    Exception - DmlRepoError
    Functions - require_ref
"""

import traceback
from dataclasses import dataclass, field
from getpass import getuser
from typing import TYPE_CHECKING, Any, Optional, Union, dataclass_transform
from uuid import uuid4

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from daggerml._internal._db import Ref
from daggerml._internal.util import now

if TYPE_CHECKING:
    from daggerml._internal.ops.base_ops import TxnContext

# Type aliases for scalar and collection data
Scalar = Optional[Union[int, float, str, bool]]
MaybeRef = Union[Scalar, Ref]  # Alias for MaybeRefScalar
MaybeRefScalar = Union[Scalar, Ref]
Collection = Union[list[Scalar], dict[str, Scalar]]
MaybeRefList = list[MaybeRefScalar]
MaybeRefDict = dict[str, MaybeRefScalar]
MaybeRefCollection = Union[MaybeRefList, MaybeRefDict]
RefCollection = Union[list[Ref], dict[str, Ref]]

# Constants
NONE = uuid4()
DEFAULT_HEAD = Ref("head:main")
DEFAULT_USER = getuser()

# Registries for object namespaces and node types
NAMESPACES: dict[str, type] = {}


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
        from dataclasses import fields

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

    def __post_init__(self):
        """Run validation after dataclass initialization.

        All DmlBase subclasses will call their `_validate` method to
        assert field types and namespaces are correct.
        """
        self._validate()


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


@dataclass
class Uri(Datum):
    """URI reference to an external location.

    Stores a URI string that points to an external location
    (e.g., S3 URI, Docker image, file path).

    Attributes
    ----------
    uri : str
        The URI string.
    """

    uri: str

    def _validate(self) -> None:
        if not isinstance(self.uri, str):
            raise TypeError(f"{self.__class__.__name__}.uri must be a string, got: {self.uri!r}")


@dataclass
class Runnable(DmlBase):
    """Public runnable value with fully materialized Python fields."""

    target: Uri
    sub: Optional["Runnable"] = None
    kwargs: dict[str, Any] = field(default_factory=dict)
    adapter: str = ""

    def _validate(self) -> None:
        tname = self.__class__.__name__
        if not isinstance(self.target, Uri):
            raise TypeError(f"{tname}.target must be Uri, got {type(self.target).__name__}")
        if self.sub is not None and not isinstance(self.sub, Runnable):
            raise TypeError(f"{tname}.sub must be Runnable or None, got {type(self.sub).__name__}")
        if not isinstance(self.kwargs, dict):
            raise TypeError(f"{tname}.kwargs must be a dict")
        for k in self.kwargs:
            if not isinstance(k, str):
                raise TypeError(f"{tname}.kwargs keys must be strings")
        if not isinstance(self.adapter, str):
            raise TypeError(f"{tname}.adapter must be a string, got: {self.adapter!r}")


@dataclass
class RunnableDatum(Datum):
    """Specification for an executable computation.

    Represents something that can be executed via an adapter, with default
    parameters. Runnables can wrap other runnables for composition
    (e.g., "run on AWS Batch" wrapping "run Python process").

    Attributes
    ----------
    target : Ref
        Reference to a Uri.
    sub : Ref | None
        Optional reference to another RunnableDatum (for wrapping).
    kwargs : Ref
        Reference to a DictDatum mapping keyword names to datum refs.
    adapter : str
        The adapter name used to execute this runnable.
    """

    __datum_name__ = "runnable"

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
        super().__post_init__()

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
    def from_ex(cls, exc) -> Self:
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

    def datum_ref(self, txn: "TxnContext") -> Ref:
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

    def datum_ref(self, txn: "TxnContext") -> Ref:
        """Get the Datum reference for this node's value.

        Parameters
        ----------
        txn : "TxnContext"
            Transaction context to resolve references.

        Returns
        -------
        Ref
            The Datum reference for this node's value.
        """
        return self.value


@dataclass
class ArgvNode(LiteralNode):
    """Special literal node representing function arguments.

    Used to mark the argv input to a function call in a DAG.
    """


@dataclass
class KwargvNode(LiteralNode):
    """Special literal node representing function keyword arguments.

    Used to mark the kwargv input to a function call in a DAG.
    The value must be a Ref to a Datum containing a dict of str->Ref(datum).
    """


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

    def datum_ref(self, txn: "TxnContext") -> Ref:
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

    def datum_ref(self, txn: "TxnContext") -> Ref:
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
            raise txn.get(dag.error)
        if dag.result is None:
            raise DmlRepoError("DAG has no result node")
        node = txn.get(dag.result)
        return node.datum_ref(txn)


@_register_dml_obj
class Dag(DmlBase):
    """Directed acyclic graph of computational nodes.

    A DAG represents a complete computation with nodes, named references,
    and an optional result node.

    Attributes
    ----------
    nodes : list[Ref]
        List of node references in this DAG.
    names : dict[str, Ref]
        Named references to nodes (variable names).
    result : Optional[Ref]
        The final result node of this computation.
    error : Optional[Ref]
        Optional reference to an error node if computation failed.
    argv : Optional[Ref]
        Optional reference to the argv node for function calls.
    """

    nodes: list[Ref]  # -> node
    names: dict[str, Ref]  # -> node
    result: Optional[Ref]  # -> node
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

    def cache_key(self, txn: "TxnContext") -> str:
        """Compute a cache key for this DAG.

        Items in the cache are stored as `cache:{key}`.

        Parameters
        ----------
        txn : "TxnContext"
            Transaction context to resolve references.

        Returns
        -------
        str
            The datum_ref.id() of the argv Datum.
        """
        if self.argv is None:
            raise DmlRepoError("Cannot compute cache key for DAG without argv.")
        argv_node = txn.get(self.argv)
        return argv_node.value.id()


@_register_dml_obj
class Tree(DmlBase):
    """Named collection of DAGs.

    A tree organizes multiple DAGs by name, typically representing
    different computations or workflow branches.

    Attributes
    ----------
    dags : dict[str, Ref]
        Mapping of names to DAG references.
    """

    dags: dict[str, Ref]  # -> dag

    def _validate(self) -> None:
        if not isinstance(self.dags, dict):
            raise TypeError("dags must be a dict of str->Ref")
        for k, v in self.dags.items():
            if not isinstance(k, str):
                raise TypeError(f"{self.__class__.__name__}.dags keys must be strings")
            require_ref(v, expected_ns=["dag"], context=f"{self.__class__.__name__}.dags[{k!r}]")


@_register_dml_obj
class Commit(DmlBase):
    """Versioned snapshot with metadata.

    A commit represents a point-in-time state of the repository,
    including the tree, DAG, authorship, and history information.

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
    dag : Optional[Ref]
        Optional reference to the DAG for this commit.
    created : str
        ISO timestamp when commit was created.
    modified : str
        ISO timestamp when commit was last modified.
    """

    parents: list[Ref]  # -> commit
    tree: Ref  # -> tree
    author: str
    message: str
    dag: Optional[Ref] = None  # -> Dag
    created: str = field(default_factory=now)
    modified: str = field(default_factory=now)

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
        if self.dag is not None:
            require_ref(self.dag, expected_ns=["dag"], context=f"{self.__class__.__name__}.dag")
        if not isinstance(self.created, str):
            msg = f"{self.__class__.__name__}.created must be an ISO timestamp string, got: {self.created!r}"
            raise TypeError(msg)
        if not isinstance(self.modified, str):
            msg = f"{self.__class__.__name__}.modified must be an ISO timestamp string, got: {self.modified!r}"
            raise TypeError(msg)


@_register_dml_obj
class Head(DmlBase):
    """Named reference to a commit.

    A head (like a Git branch) tracks the current commit for a line of work.

    Attributes
    ----------
    commit : Ref
        Reference to the current commit.
    """

    commit: Ref  # -> commit

    def _validate(self) -> None:
        require_ref(self.commit, expected_ns=["commit"], context=f"{self.__class__.__name__}.commit")


@_register_dml_obj
class Index(DmlBase):
    """Working index for uncommitted changes.

    An index represents the current state of changes before they are committed.
    It tracks the current commit that the index is based on.

    Attributes
    ----------
    commit : Ref
        Reference to the base commit.
    """

    commit: Ref  # -> commit

    def _validate(self) -> None:
        require_ref(self.commit, expected_ns=["commit"], context=f"{self.__class__.__name__}.commit")
