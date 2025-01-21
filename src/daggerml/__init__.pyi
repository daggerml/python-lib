"""
DaggerML - A Python library for building and managing directed acyclic graphs.

This library provides tools for creating, manipulating, and executing DAGs
with strong typing support and a context-manager based interface.
"""
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Callable, Dict, Iterator, List, Optional, Type, Union, overload

Scalar = Union[str, int, float, bool, None, Resource, "Node"]
Collection = Union[list, tuple, set, dict]

__version__: str

def new(name: str, message: str, data: Optional[str] = None) -> 'Dag':
    """
    Create a new DAG with the given name and message.

    Parameters
    ----------
    name : str
        Name of the DAG
    message : str
        Commit message or description
    data : str, optional
        Serialized DAG data for initialization

    Returns
    -------
    Dag
        A new DAG instance
    """
    ...

def from_json(text: str) -> Any:
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
    ...

def to_json(obj: Any) -> str:
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
    ...

@dataclass(frozen=True)
class Ref:
    """
    Reference to a DaggerML node.

    Parameters
    ----------
    to : str
        Reference identifier
    """
    to: str

@dataclass(frozen=True, slots=True)
class Resource:
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

@dataclass
class Error(Exception):
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
    context: Dict[str, Any] = field(default_factory=dict)
    code: Optional[str] = None
    
    def __str__(self) -> str: ...

class Dml:
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
    
    def __init__(self, *, data: Optional[Any] = None, 
                 message_handler: Optional[Callable] = None, **kwargs: Any) -> None: ...
    
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
        ...
    def __enter__(self) -> 'Dml': ...
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None: ...
    
    def new(self, name: str, message: str) -> 'Dag':
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
        ...

@dataclass
class Dag:
    """
    Representation of a DaggerML DAG.

    Parameters
    ----------
    dml : Dml
        DaggerML instance
    token : str
        DAG token
    dump : str, optional
        Serialized DAG data
    message_handler : callable, optional
        Function to handle messages
    """
    dml: Dml
    token: str
    dump: Optional[str] = None
    message_handler: Optional[Callable] = None
    
    def __enter__(self) -> 'Dag': ...
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None: ...
    
    @property
    def expr(self) -> "Node":
        """
        Get the root expression node of the DAG.

        Returns
        -------
        Node
            Root expression node
        """
        ...
    
    def put(self, value: Union[Scalar, Collection], *, 
            name: Optional[str] = None, doc: Optional[str] = None) -> "Node":
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
        ...
    
    def load(self, dag_name: str, *, 
             name: Optional[str] = None,
             doc: Optional[str] = None) -> "Node":
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
        ...
    
    def commit(self, value: Union["Node", Error, Any]) -> None:
        """
        Commit a value to the DAG.

        Parameters
        ----------
        value : Union[Node, Error, Any]
            Value to commit
        """
        ...

@dataclass(frozen=True)
class Node:
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
    
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    
    @overload
    def __getitem__(self, key: slice) -> List[Node]: ...
    @overload
    def __getitem__(self, key: Union[str, int]) -> Node: ...
    @overload
    def __getitem__(self, key: Node) -> Node: ...
    
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Union[Node, str]]: ...
    
    def __call__(self, *args: Any,
                 name: Optional[str] = None, 
                 doc: Optional[str] = None,
                 timeout: int = 30000) -> Node:
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
        """
        ...
    
    def keys(self, *, name: Optional[str] = None, doc: Optional[str] = None) -> Node:
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
        ...
    
    def len(self, *, name: Optional[str] = None, doc: Optional[str] = None) -> Node:
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
        ...
    
    def type(self, *, name: Optional[str] = None, doc: Optional[str] = None) -> Node:
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
        ...
    
    def items(self) -> Iterator[tuple[Node, Node]]:
        """
        Iterate over key-value pairs of a dictionary node.

        Returns
        -------
        Iterator[tuple[Node, Node]]
            Iterator over (key, value) pairs
        """
        ...
    
    def value(self) -> Any:
        """
        Get the concrete value of this node.

        Returns
        -------
        Any
            The actual value represented by this node
        """
        ...
