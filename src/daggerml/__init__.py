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
    return Dml().new(name, message)


new.__doc__ = Dml.new.__doc__


def load(name) -> Dag:
    return Dml().load(name)


__all__ = ("Dml", "Dag", "Error", "Node", "Ref", "Resource", "from_json", "to_json")
