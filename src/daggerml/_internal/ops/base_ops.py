"""Foundation class providing core repository operations shared across all subsystems.

This module provides BaseOps, a base class that encapsulates common database
operations used by all repository subsystems. It handles transactions,
object storage/retrieval, and reference management.

Public API:
    BaseOps - Base class for repository operations
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional, cast

from daggerml._internal._db import (
    DmlDbEnv,
    DmlDbEnvReopenedError,
    DmlDbEnvTxn,
    DmlDbKeyNotFoundError,
    DmlDbMapFullError,
    Ref,
)
from daggerml._internal.types import NAMESPACES, Commit, Dag, Deletable, DmlRepoError, Error, Tree, Uri


@dataclass
class CommitCtx:
    """Context helper for accessing commit/tree/dag state without pointers."""

    commit: Commit
    tree: Tree
    dag: Dag = cast(Dag, None)


def with_retry(fn):
    """Decorator to retry transactions on recoverable database errors.

    Handles two types of recoverable errors:
    1. DmlDbMapFullError: Database is full - automatically resize and retry
    2. DmlDbEnvReopenedError: Environment was repaired (e.g., after fork/EINVAL) - retry transaction

    Parameters
    ----------
    fn : Callable
        The function to wrap.

    Returns
    -------
    Callable
        The wrapped function.
    """

    def wrapper(self, *args, **kwargs):
        retries = 0
        max_retries = 8
        while True:
            try:
                return fn(self, *args, **kwargs)
            except DmlDbMapFullError:
                map_size = self._db.get_size()
                new_map_size = map_size * 2
                logging.warning("Resizing database from %d to %d bytes", map_size, new_map_size)
                self._db.resize(new_map_size)
                retries += 1
                if retries >= max_retries:
                    raise
            except DmlDbEnvReopenedError as e:
                # Environment was repaired (all transactions invalidated), retry the operation
                retries += 1
                if retries >= max_retries:
                    raise
                logging.info(
                    "Database environment reopened, retrying transaction (attempt %d/%d): %s", retries, max_retries, e
                )

    return wrapper


@dataclass
class TxnContext:
    """Transaction context for BaseOps operations.

    Attributes
    ----------
    txn : DmlDbEnvTxn
        The active database transaction.
    logger : logging.Logger
        Logger for the transaction context.
    """

    db: DmlDbEnv
    txn: DmlDbEnvTxn
    logger: logging.Logger

    def put(self, obj: Any, to: Optional[Ref] = None) -> Ref:
        """Store object and return its reference.

        Parameters
        ----------
        obj : Any
            Object to store.
        to : Optional[Ref]
            Optional reference to store at. If None, generates new reference.

        Returns
        -------
        Ref
            Reference to the stored object.

        Raises
        ------
        DmlRepoError
            If the object cannot be stored.
        """
        if isinstance(obj, Ref):
            # For Ref objects, we store them directly without validation/serialization
            # This is used for head pointers that store Ref values
            ns = to.ns() if to else None
            if ns and ns not in self.db.namespaces:
                raise ValueError(f"Unknown namespace: {ns}")
            return self.txn.put(obj, ns=ns, to=to)
        obj._validate()
        ns = None
        if to is None:
            ns = obj._ns
        if (ns or to.ns()) not in self.db.namespaces:
            raise ValueError(f"Unknown namespace: {ns}")
        ref = self.txn.put(obj.to_dict(), ns=ns, to=to)
        if isinstance(obj, Uri):
            self._cleanup_opposite_entry(ref, opposite_ns="deletable", noun="uri")
        elif isinstance(obj, Deletable):
            self._cleanup_opposite_entry(ref, opposite_ns="datum-uri", noun="deletable")
        return ref

    def get(self, ref: Ref) -> Any:
        """Retrieve object by reference.

        Parameters
        ----------
        ref : Ref
            Reference to the object to retrieve.

        Returns
        -------
        Any
            The object stored at the reference.

        Raises
        ------
        DmlRepoError
            If the reference is invalid or object cannot be retrieved.
        """
        try:
            ns = ref.ns()
            cls = NAMESPACES.get(ns)
            if cls is None:
                raise ValueError(f"Unknown namespace: {ns}")
            obj = cls.from_dict(self.txn.get(ref))
            return obj
        except DmlDbKeyNotFoundError:
            # Explicitly silence exception chaining for not-found case
            raise DmlRepoError(f"Object not found: {ref}") from None
        except Error:
            self.logger.exception(f"get: ref={ref}")
            raise
        except Exception as e:
            self.logger.exception(f"get: ref={ref}")
            raise DmlRepoError(f"Failed to get object: {e}") from e

    def delete(self, ref: Ref) -> None:
        """Delete object at reference.

        Parameters
        ----------
        ref : Ref
            Reference to the object to delete.

        Raises
        ------
        DmlRepoError
            If the object cannot be deleted.
        """
        try:
            self.txn.delete(ref)
        except Exception as e:
            self.logger.exception(f"delete: ref={ref}")
            raise DmlRepoError(f"Failed to delete object: {e}") from e

    def exists(self, ref: Ref) -> bool:
        """Check if object exists at reference.

        Parameters
        ----------
        ref : Ref
            Reference to check.

        Returns
        -------
        bool
            True if object exists, False otherwise.

        Raises
        ------
        DmlRepoError
            If existence check fails.
        """
        try:
            return self.txn.exists(ref)
        except Exception as e:
            self.logger.exception(f"exists: ref={ref}")
            raise DmlRepoError(f"Failed to check object existence: {e}") from e

    def iter(self, namespace: str) -> Iterator[Ref]:
        """Iterate over objects in a namespace.

        Parameters
        ----------
        namespace : str
            Namespace to iterate over.

        Yields
        ------
        Ref
            References to objects in the namespace.

        Raises
        ------
        DmlRepoError
            If iteration fails.
        """
        try:
            for entry in self.txn.iter(namespace):
                if isinstance(entry, tuple):
                    yield entry[0]
                else:
                    yield entry
        except DmlDbKeyNotFoundError:
            self.logger.info("No objects found in the repository.")
        except Exception as e:
            self.logger.exception(f"iter: namespace={namespace}")
            raise DmlRepoError(f"Failed to iterate over namespace '{namespace}': {e}") from e

    def _cleanup_opposite_entry(self, ref: Ref, *, opposite_ns: str, noun: str) -> None:
        """Remove a stale opposite entry for the given reference.

        Parameters
        ----------
        ref : Ref
            Reference to clean up opposite entries for.
        opposite_ns : str
            Namespace of the opposite entry to remove.
        noun : str
            Object type for logging context.
        """
        try:
            opposite_ref = Ref(f"{opposite_ns}:{ref.id()}")
            if self.txn.exists(opposite_ref):
                self.logger.warning(
                    "Clearing %s entry %s to keep %s/%s mutually exclusive (new: %s)",
                    noun,
                    opposite_ref,
                    ref.ns(),
                    opposite_ns,
                    ref,
                )
                self.txn.delete(opposite_ref)
        except Exception as e:
            self.logger.exception(f"cleanup_opposite_entry: ref={ref}")
            raise DmlRepoError(f"Failed to cleanup opposite entry: {e}") from e

    def get_commit_ctx(self, commit_ref: Ref) -> CommitCtx:
        """Create context helper from a commit reference."""

        commit: Commit = self.get(commit_ref)
        tree: Tree = self.get(commit.tree)
        dag = commit.dag and self.get(commit.dag)
        return CommitCtx(commit, tree, dag=cast(Dag, dag))


@dataclass
class BaseOps:
    """Foundation class providing core repository operations.

    This class encapsulates common database operations and provides
    a consistent interface for all repository subsystems. It handles
    transactions, object storage/retrieval, and reference management.

    It is intended as a helper base class and should not be used directly.
    None of its methods nor attributes are part of the public API.

    Attributes
    ----------
    db : DmlDbEnv
        Database environment instance.
    """

    _db: DmlDbEnv

    def __post_init__(self):
        """Initialize logger after dataclass initialization."""
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @contextmanager
    def _tx(self, readonly: bool = False):
        """Transaction context manager.

        Provides a transaction context that can be used to perform multiple
        operations atomically. Temporarily binds this BaseOps instance to the
        transaction-bound database for use with other operations.

        Parameters
        ----------
        readonly : bool
            If True, opens a read-only transaction. Default is False.

        Yields
        ------
        Self
            The BaseOps instance bound to the transaction.

        Raises
        ------
        DmlRepoError
            If transaction cannot be created or operations fail.
        """
        try:
            with self._db.tx(readonly=readonly) as txn:
                self._logger.debug("Nested transactions are not supported. Readonly flag will be ignored.")
                yield TxnContext(db=self._db, txn=txn, logger=self._logger)
        except DmlDbMapFullError:
            # Allow upstream retry managers (e.g. with_retry) to replay the full txn.
            raise
        except DmlDbEnvReopenedError:
            # Allow upstream retry managers (e.g. with_retry) to replay the full txn.
            raise
        except Error:
            raise
        except Exception as e:
            self._logger.exception(f"Transaction failed: readonly={readonly}")
            raise DmlRepoError(f"Transaction failed: {e}") from e
