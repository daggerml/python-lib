"""
DaggerML - A Python library for building and managing directed acyclic graphs.

This library provides tools for creating, manipulating, and executing DAGs
with strong typing support and a context-manager based interface.
"""

from daggerml.core import Dml, Resource

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"


def new(name, message, **kwargs):
    """
    Create a new DAG with the given name and message.

    Parameters
    ----------
    name : str
        Name of the DAG
    message : str
        Commit message or description

    Returns
    -------
    Dag
        A new DAG instance
    """
    return Dml(**kwargs).new(name, message)


__all__ = ("Dml", "Resource")
