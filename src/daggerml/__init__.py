"""
DaggerML - A Python library for building and managing directed acyclic graphs.

This library provides tools for creating, manipulating, and executing DAGs
with strong typing support and a context-manager based interface.
"""

from daggerml.core import Dag, Dml, Error, Node, Ref, Resource, from_json, to_json

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"


def new(name, message):
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
    >>> import daggerml as dml
    >>> dag = dml.new("dag-0", "message")
    >>> dag.numbers = [1, 2, 3, 4, 5]  # add data directly
    >>> dag.mapping = {"x": dag.numbers, "y": "string value"}
    >>> # Add nodes using dictionary syntax
    >>> dag['&alt_numbers'] = dag.numbers  # for those more difficult names
    >>> assert dag.numbers.value() == [1, 2, 3, 4, 5]  # get data back
    >>> assert dag.mapping['x'].value() == [1, 2, 3, 4, 5]  # access values while preseving topology
    >>> dag.result = dag.numbers  # commit the value

    Load past DAGs
    >>> dag = dml.new("dag-1", "another message")
    >>> old_dag = dml.load("dag-0")
    >>> dag.a = old_dag["&alt_numbers"]

    Capture errors with the context manager usage
    >>> try:
    ...     with dag:
    ...         1 / 0
    ... except ZeroDivisionError as e:
    ...     print(str(e))
    division by zero

    Loading an error raises an error
    >>> dag = dml.new("dag-2", "yet another message")
    >>> failed_dag = dml.load("dag-1")
    >>> dag.x = failed_dag.a  # loading nodes without errors is fine
    >>> dag.x.value()
    [1, 2, 3, 4, 5]
    >>> try:
    ...     failed_dag.result  # the value is itself an error
    ... except dml.Error as e:
    ...     print(e.message)
    division by zero
    """
    return Dml().new(name, message)


Dml.new.__doc__ = new.__doc__
Dml.__doc__ = """
Main DaggerML interface for creating and managing DAGs.

Parameters
----------
data : Any, optional
    Initial data for the DML instance
message_handler : callable, optional
    Function to handle messages during DAG operations
**kwargs : dict
    Additional configuration options including:
    - config_dir: Configuration directory path
    - project_dir: Project directory path
    - repo: Repository name
    - user: Username
    - branch: Branch name

Notes
-----
* When used as a context manager, it creates temporary configuration and project
  directories, and handles cleanup automatically.
* You only need to use this if you want to use a different repo, or something like that.

Examples
--------
>>> # Basic initialization and usage
>>> with Dml() as dml:
...     status = dml('status')  # Call CLI commands directly
...     # Create a new DAG
...     with dml.new("my_dag", "Initial commit") as dag:
...         dag.x = [1, 2, 3]  # Add nodes to DAG
...         dag.result = dag.x  # Set result
...     # Load and use an existing DAG
...     loaded = dml.load("my_dag")
...     assert loaded.x.value() == [1, 2, 3]
"""


def load(name) -> Dag:
    return Dml().load(name)


__all__ = ("Dml", "Dag", "Error", "Node", "Ref", "Resource", "from_json", "to_json")
