"""
DaggerML - A Python library for building and managing directed acyclic graphs.

This library provides tools for creating, manipulating, and executing DAGs
with strong typing support and a context-manager based interface.
"""

from daggerml.core import Dml, Error, Node, Resource

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"


__all__ = ("Dml", "Error", "Node", "Resource")
