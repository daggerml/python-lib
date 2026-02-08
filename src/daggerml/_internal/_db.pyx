# cython: language_level=3
"""
Standalone Cython extension module for database operations.

This module wraps the underlying C library (dml_db) that handles:
- LMDB database operations
- Data serialization/deserialization
- Reference management
- Transaction handling
"""
import logging
import os
import sys
import threading
import base64
from contextlib import contextmanager
from libc.stdlib cimport malloc, free, calloc
from libc.string cimport strlen, memcpy

from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from cpython.float cimport PyFloat_AsDouble
from cpython.long cimport PyLong_AsLongLongAndOverflow
from cpython.unicode cimport PyUnicode_AsUTF8, PyUnicode_AsUTF8AndSize, PyUnicode_DecodeUTF8
from cpython.exc cimport PyErr_Clear, PyErr_Occurred


logger = logging.getLogger(__name__)
def _config_int(name, default):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


MAX_STRING_BYTES = _config_int("DML_DB_MAX_STRING_BYTES", 1024 * 1024)
MAX_COLLECTION_LEN = _config_int("DML_DB_MAX_COLLECTION_LEN", 100000)

cdef extern from "dml_value.h":
    ctypedef enum DmlValueType:
        DML_VALUE_NULL
        DML_VALUE_BOOL
        DML_VALUE_INT
        DML_VALUE_FLOAT
        DML_VALUE_STR
        DML_VALUE_LIST
        DML_VALUE_MAP
        DML_VALUE_REF

    ctypedef struct DmlValue

    ctypedef struct DmlMapEntry:
        char *key
        size_t key_len
        DmlValue *value

    ctypedef struct DmlValueStr:
        char *data
        size_t size

    ctypedef struct DmlValueList:
        DmlValue **items
        size_t count

    ctypedef struct DmlValueMap:
        DmlMapEntry *entries
        size_t count

    ctypedef union DmlValueAs:
        int boolean
        long long integer
        double floating
        DmlValueStr str
        DmlValueList list
        DmlValueMap map
        DmlValueStr ref

    ctypedef struct DmlValue:
        DmlValueType type
        DmlValueAs as

    DmlValue *dml_value_new_null() nogil
    DmlValue *dml_value_new_bool(int value) nogil
    DmlValue *dml_value_new_int(long long value) nogil
    DmlValue *dml_value_new_float(double value) nogil
    DmlValue *dml_value_new_str(const char *data, size_t size) nogil
    DmlValue *dml_value_new_ref(const char *data, size_t size) nogil
    DmlValue *dml_value_new_list(size_t count) nogil
    int dml_value_list_set(DmlValue *list, size_t index, DmlValue *item) nogil
    DmlValue *dml_value_new_map(size_t count) nogil
    int dml_value_map_set(DmlValue *map, size_t index, const char *key, size_t key_len, DmlValue *value) nogil
    void dml_value_free(DmlValue *value) nogil
    int dml_ref_split(
        const char *ref,
        size_t ref_len,
        const char **namespace_str,
        size_t *namespace_len,
        const char **id_str,
        size_t *id_len
    ) nogil

cdef extern from "dml_db.h":
    int DML_DB_ERR_HANDLE_INVALID
    int DML_DB_ERR_HANDLE_CLOSED
    int DML_DB_ERR_HANDLE_FORKED
    int DML_DB_ERR_TXN_INVALID
    int DML_DB_ERR_TXN_READONLY
    int DML_DB_ERR_TXN_FORKED
    int DML_DB_ERR_INPUT_INVALID
    int DML_DB_ERR_TYPE_INVALID
    int DML_DB_ERR_PATH_INVALID
    int DML_DB_ERR_REF_INVALID
    int DML_DB_ERR_NAMESPACE_INVALID
    int DML_DB_ERR_NOT_FOUND
    int DML_DB_ERR_KEY_EXISTS
    int DML_DB_ERR_MSGPACK
    int DML_DB_ERR_NOMEM
    int DML_DB_ERR_MAP_FULL
    int DML_DB_ERR_BUSY
    int DML_DB_ERR_LMDB
    int DML_DB_ERR_INTERNAL
    int DML_DB_ERR_ENV_REOPENED

    ctypedef struct DmlDbHandle:
        pass

    ctypedef struct DmlDbTxn:
        pass

    ctypedef struct DmlObjCollection:
        char *keys
        size_t *key_lens
        DmlValue **values
        size_t count
        char *next_token

    int dml_db_open(
        const char *path,
        const char *const *namespaces,
        size_t namespace_count,
        const int create_if_missing,
        size_t map_size,
        DmlDbHandle **out_handle,
    ) nogil
    int dml_db_close(DmlDbHandle **p_handle) nogil

    int dml_db_mapsize(DmlDbHandle **p_handle, size_t *out_mapsize) nogil
    int dml_db_resize(DmlDbHandle **p_handle, size_t mapsize) nogil

    int dml_db_txn_begin(DmlDbHandle **p_handle, const int readonly, DmlDbTxn **out_txn);
    int dml_db_txn_fin(DmlDbHandle **p_handle, DmlDbTxn *txn, const int commit) nogil

    int dml_db_put(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len,
        const DmlValue *value,
        int no_overwrite,
        int raw,
        DmlValue **out_ref
    ) nogil
    int dml_db_get(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len,
        int raw,
        DmlValue **out_value
    ) nogil
    int dml_db_del(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len
    ) nogil
    int dml_db_exists(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len,
        int *out_exists
    ) nogil

    int dml_db_iter_keys(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *ns,
        const char *start_token,
        DmlObjCollection *out_page
    ) nogil
    void dml_db_free_obj_collection(DmlObjCollection *page) nogil
    int dml_db_list_orphans(
        DmlDbHandle **p_handle,
        DmlDbTxn *txn,
        const char *const *start_refs,
        size_t start_refs_count,
        DmlValue **out_refs
    ) nogil

cdef DmlValue *py_to_dml_value(object obj):
    cdef DmlValue *result
    cdef DmlValue *child
    cdef DmlValue *data_value
    cdef Py_ssize_t size
    cdef const char *data
    cdef Py_ssize_t i
    cdef long long int_val
    cdef int overflow
    # keep Python-level references to temporary Unicode objects
    cdef object py_str
    cdef object py_to_obj
    cdef object py_map_key

    if obj is None:
        result = dml_value_new_null()
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, bool):
        result = dml_value_new_bool(1 if obj else 0)
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, int):
        overflow = 0
        int_val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if overflow != 0:
            raise DmlDbInvalidTypeError(
                f"invalid int value for db storage: {obj!r} "
                "(reason: out of range for int64)"
            )
            return NULL
        if PyErr_Occurred() != NULL:
            PyErr_Clear()
            raise DmlDbInvalidTypeError(
                f"invalid int value for db storage: {obj!r} "
                "(reason: conversion failed)"
            )
            return NULL
        result = dml_value_new_int(int_val)
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, float):
        result = dml_value_new_float(PyFloat_AsDouble(obj))
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, str):
        py_str = obj
        data = PyUnicode_AsUTF8AndSize(py_str, &size)
        if data == NULL:
            return NULL
        if size > MAX_STRING_BYTES:
            raise DmlDbInvalidTypeError(
                f"invalid string value for db storage: {obj!r} "
                f"(reason: exceeds max length {MAX_STRING_BYTES})"
            )
        result = dml_value_new_str(data, <size_t>size)
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, Ref):
        py_to_obj = obj.to
        data = PyUnicode_AsUTF8AndSize(py_to_obj, &size)
        if data == NULL:
            return NULL
        result = dml_value_new_ref(data, <size_t>size)
        if result == NULL:
            raise MemoryError()
        return result
    if isinstance(obj, (list, tuple)):
        size = len(obj)
        if size > MAX_COLLECTION_LEN:
            raise DmlDbInvalidTypeError(
                f"invalid list value for db storage: {obj!r} "
                f"(reason: exceeds max length {MAX_COLLECTION_LEN})"
            )
        result = dml_value_new_list(<size_t>size)
        if result == NULL:
            raise MemoryError()
        try:
            for i in range(size):
                child = py_to_dml_value(obj[i])
                if child == NULL:
                    dml_value_free(result)
                    raise MemoryError("Failed to convert list item")
                if dml_value_list_set(result, <size_t>i, child) != 0:
                    raise DmlDbInvalidTypeError(
                        f"invalid list entry for db storage: {obj[i]!r} "
                        "(reason: unsupported type)"
                    )
            return result
        except Exception:
            dml_value_free(result)
            raise(obj)
    if isinstance(obj, dict):
        size = len(obj)
        if size > MAX_COLLECTION_LEN:
            raise DmlDbInvalidTypeError(
                f"invalid dict value for db storage: {obj!r} "
                f"(reason: exceeds max length {MAX_COLLECTION_LEN})"
            )
        result = dml_value_new_map(<size_t>size)
        if result == NULL:
            raise MemoryError()
        i = 0
        for key, value in obj.items():
            if not isinstance(key, str):
                dml_value_free(result)
                raise DmlDbInvalidTypeError(
                    f"invalid dict key for db storage: {key!r} "
                    "(reason: keys must be str)"
                )
            py_map_key = key
            data = PyUnicode_AsUTF8AndSize(py_map_key, &size)
            if data == NULL:
                dml_value_free(result)
                return NULL
            child = py_to_dml_value(value)
            if child == NULL:
                dml_value_free(result)
                return NULL
            if dml_value_map_set(result, <size_t>i, data, <size_t>size, child) != 0:
                dml_value_free(child)
                dml_value_free(result)
                raise DmlDbInvalidTypeError(
                    f"invalid dict value for db storage: {value!r} "
                    "(reason: unsupported type)"
                )
            i += 1
        return result
    raise DmlDbInvalidTypeError(
        f"invalid value for db storage: {obj!r} "
        f"(reason: unsupported type {type(obj).__name__})"
    )
    return NULL

cdef object dml_value_to_py(const DmlValue *value):
    cdef size_t i
    cdef object py_obj
    cdef object py_key

    if value == NULL:
        raise ValueError("Invalid MessagePack payload")

    if value.type == DML_VALUE_NULL:
        return None
    if value.type == DML_VALUE_BOOL:
        return bool(value.as.boolean)
    if value.type == DML_VALUE_INT:
        return int(value.as.integer)
    if value.type == DML_VALUE_FLOAT:
        return float(value.as.floating)
    if value.type == DML_VALUE_STR:
        return PyUnicode_DecodeUTF8(value.as.str.data, <Py_ssize_t>value.as.str.size, "replace")
    if value.type == DML_VALUE_REF:
        py_obj = PyUnicode_DecodeUTF8(value.as.ref.data, <Py_ssize_t>value.as.ref.size, "strict")
        return Ref(py_obj)
    if value.type == DML_VALUE_LIST:
        py_list = []
        for i in range(value.as.list.count):
            py_list.append(dml_value_to_py(value.as.list.items[i]))
        return py_list
    if value.type == DML_VALUE_MAP:
        py_dict = {}
        for i in range(value.as.map.count):
            entry = &value.as.map.entries[i]
            py_key = PyUnicode_DecodeUTF8(entry.key, <Py_ssize_t>entry.key_len, "strict")
            py_dict[py_key] = dml_value_to_py(entry.value)
        return py_dict
    raise TypeError("Unsupported msgpack object type")

class DmlDbError(Exception):
    """
    Base error for database operations.

    Notes
    -----
    Subclasses provide specific failure details.
    """
    pass

class DmlDbInvalidHandleError(DmlDbError):
    """
    Invalid database handle.

    Notes
    -----
    Raised when a handle is NULL or uninitialized.
    """
    pass

class DmlDbClosedError(DmlDbError):
    """
    Database handle is closed.

    Notes
    -----
    Raised when operations use a closed handle.
    """
    pass

class DmlDbForkedError(DmlDbError):
    """
    Database handle used after fork without reopen.

    Notes
    -----
    Raised when a handle is reused across fork boundaries.
    """
    pass

class DmlDbInvalidTxnError(DmlDbError):
    """
    Transaction is invalid or closed.

    Notes
    -----
    Raised when a transaction pointer is NULL or closed.
    """
    pass

class DmlDbReadonlyTxnError(DmlDbError):
    """
    Write attempted in a read-only transaction.

    Notes
    -----
    Raised when write operations run in read-only transactions.
    """
    pass

class DmlDbForkedTxnError(DmlDbError):
    """
    Transaction used after a fork.
    """
    pass

class DmlDbInvalidInputError(ValueError, DmlDbError):
    """
    Invalid input supplied to a database call.

    Notes
    -----
    Covers null pointers and empty strings where disallowed.
    """
    pass

class DmlDbInvalidTypeError(ValueError, DmlDbError):
    """
    Input type is not representable in the database.

    Notes
    -----
    Raised for Python types or values that cannot be encoded.
    """
    pass

class DmlDbInvalidPathError(ValueError, DmlDbError):
    """
    Database path is invalid or inaccessible.

    Notes
    -----
    Raised when the filesystem path does not exist or cannot be opened.
    """
    pass

class DmlDbInvalidRefError(ValueError, DmlDbError):
    """
    Invalid reference format.

    Notes
    -----
    Raised when ref parsing fails.
    """
    pass

class DmlDbInvalidNamespaceError(ValueError, DmlDbError):
    """
    Namespace is not configured or allowed.

    Notes
    -----
    Raised when the namespace is missing from the configured list.
    """
    pass

class DmlDbKeyNotFoundError(DmlDbError):
    """
    Database key does not exist.

    Notes
    -----
    Raised when a lookup fails to find a key.
    """
    pass

class DmlDbKeyExistsError(DmlDbError):
    """
    Database key already exists.

    Notes
    -----
    Raised when `no_overwrite` is set and the key is present.
    """
    pass

class DmlDbMsgpackError(DmlDbError):
    """
    MessagePack encoding or decoding failed.

    Notes
    -----
    Raised when serialization or deserialization fails.
    """
    pass

class DmlDbOutOfMemoryError(MemoryError, DmlDbError):
    """
    Memory allocation failed in the database layer.

    Notes
    -----
    Raised on allocation failures.
    """
    pass

class DmlDbMapFullError(DmlDbError):
    """
    LMDB map is full; resize is required.

    Notes
    -----
    Raised when LMDB reports a full map.
    """
    pass

class DmlDbBusyError(DmlDbError):
    """
    Database is busy and could not acquire a lock.

    Notes
    -----
    Raised when LMDB reports a busy/locked state.
    """
    pass

class DmlDbLmdbError(DmlDbError):
    """
    Unclassified LMDB backend error.

    Notes
    -----
    Raised for LMDB failures without a more specific mapping.
    """
    pass

class DmlDbInternalError(DmlDbError):
    """
    Internal invariant failure in the database layer.

    Notes
    -----
    Raised for unexpected internal failures.
    """
    pass

class DmlDbEnvReopenedError(DmlDbError):
    """
    Database environment was reopened; transaction must be retried.

    Notes
    -----
    Raised when the environment was repaired (e.g., after fork or EINVAL),
    invalidating all existing transactions. Caller should retry the entire
    transaction block.
    """
    pass

cdef inline object raise_if_error(int rc, str context):
    cls = RuntimeError
    if rc == 0:
        return
    elif rc == DML_DB_ERR_HANDLE_INVALID:
        cls = DmlDbInvalidHandleError
        prefix = "database handle is invalid"
    elif rc == DML_DB_ERR_HANDLE_CLOSED:
        cls = DmlDbClosedError
        prefix = "database handle is closed"
    elif rc == DML_DB_ERR_HANDLE_FORKED:
        cls = DmlDbForkedError
        prefix = "database handle used after fork; call DB.reopen()"
    elif rc == DML_DB_ERR_TXN_INVALID:
        cls = DmlDbInvalidTxnError
        prefix = "invalid or closed transaction"
    elif rc == DML_DB_ERR_TXN_READONLY:
        cls = DmlDbReadonlyTxnError
        prefix = "read-only transaction"
    elif rc == DML_DB_ERR_TXN_FORKED:
        cls = DmlDbForkedTxnError
        prefix = "transaction used after fork"
    elif rc == DML_DB_ERR_INPUT_INVALID:
        cls = DmlDbInvalidInputError
        prefix = "invalid input"
    elif rc == DML_DB_ERR_TYPE_INVALID:
        cls = DmlDbInvalidTypeError
        prefix = "invalid input type"
    elif rc == DML_DB_ERR_PATH_INVALID:
        cls = DmlDbInvalidPathError
        prefix = "invalid database path"
    elif rc == DML_DB_ERR_REF_INVALID:
        cls = DmlDbInvalidRefError
        prefix = "invalid ref format"
    elif rc == DML_DB_ERR_NAMESPACE_INVALID:
        cls = DmlDbInvalidNamespaceError
        prefix = "invalid namespace"
    elif rc == DML_DB_ERR_NOT_FOUND:
        cls = DmlDbKeyNotFoundError
        prefix = "data not found"
    elif rc == DML_DB_ERR_KEY_EXISTS:
        cls = DmlDbKeyExistsError
        prefix = "key already exists"
    elif rc == DML_DB_ERR_MSGPACK:
        cls = DmlDbMsgpackError
        prefix = "msgpack serialization error"
    elif rc == DML_DB_ERR_NOMEM:
        cls = DmlDbOutOfMemoryError
        prefix = "out of memory"
    elif rc == DML_DB_ERR_MAP_FULL:
        cls = DmlDbMapFullError
        prefix = "database map is full"
    elif rc == DML_DB_ERR_BUSY:
        cls = DmlDbBusyError
        prefix = "database is busy"
    elif rc == DML_DB_ERR_LMDB:
        cls = DmlDbLmdbError
        prefix = "lmdb error"
    elif rc == DML_DB_ERR_INTERNAL:
        cls = DmlDbInternalError
        prefix = "internal database error"
    elif rc == DML_DB_ERR_ENV_REOPENED:
        cls = DmlDbEnvReopenedError
        prefix = "database environment was reopened; retry transaction"
    else:
        prefix = f"unknown database error: {rc}"
    raise cls(f"{prefix}: {context}")

cdef class Ref:
    """
    Reference to another node.

    Attributes
    ----------
    to
        Reference target.

    Notes
    -----
    `Ref` distinguishes stored references from plain strings, which is needed
    so serialization can round-trip graph edges instead of raw text.
    """
    cdef public object to

    def __init__(self, to):
        """
        Initialize a reference wrapper.

        Parameters
        ----------
        to
            Reference string in `namespace/id` form.

        Raises
        ------
        TypeError
            If `to` is not a string.
        """
        if not isinstance(to, str):
            raise TypeError("to must be str")
        self.to = to

    def __repr__(self):
        """
        Return a helpful representation for debugging.

        Returns
        -------
        str
            Debug representation showing the reference target.
        """
        if self.to is None:
            return "Ref(<invalid>)"
        return f"Ref({self.to})"

    def __richcmp__(self, other, int op):
        """
        Compare references based on their target strings.

        Parameters
        ----------
        other
            Another `Ref` instance.
        op
            Comparison opcode provided by Cython.

        Returns
        -------
        object
            Comparison result or `NotImplemented` for unsupported types.
        """
        if not isinstance(other, Ref):
            return NotImplemented
        if op == 0:   return self.to < other.to
        if op == 1:   return self.to <= other.to
        if op == 2:   return self.to == other.to
        if op == 3:   return self.to != other.to
        if op == 4:   return self.to >  other.to
        if op == 5:   return self.to >= other.to
        return NotImplemented

    def __hash__(self):
        """
        Hash the reference based on its target string.

        Returns
        -------
        int
            Hash value suitable for dict/set membership.
        """
        return hash(self.to)

    def ns(self):
        """
        Return the namespace portion of the reference.

        Returns
        -------
        str
            Namespace extracted from the reference.

        Raises
        ------
        ValueError
            If the reference format is invalid.

        Notes
        -----
        This uses the database C parser so Python and C agree on ref structure.
        """
        cdef const char *data
        cdef Py_ssize_t size = 0
        cdef const char *ns = NULL
        cdef const char *ident = NULL
        cdef size_t ns_len = 0
        cdef size_t id_len = 0

        data = PyUnicode_AsUTF8AndSize(self.to, &size)
        if data == NULL:
            return None
        if dml_ref_split(data, <size_t>size, &ns, &ns_len, &ident, &id_len) != 0:
            raise ValueError("Invalid Ref format")
        return PyUnicode_DecodeUTF8(ns, <Py_ssize_t>ns_len, "strict")

    def id(self):
        """
        Return the identifier portion of the reference.

        Returns
        -------
        str
            Identifier extracted from the reference.

        Raises
        ------
        ValueError
            If the reference format is invalid.

        Notes
        -----
        This complements `ns()` by exposing the ID while keeping the split
        logic centralized in the database layer.
        """
        cdef const char *data
        cdef Py_ssize_t size = 0
        cdef const char *ns = NULL
        cdef const char *ident = NULL
        cdef size_t ns_len = 0
        cdef size_t id_len = 0

        data = PyUnicode_AsUTF8AndSize(self.to, &size)
        if data == NULL:
            return None
        if dml_ref_split(data, <size_t>size, &ns, &ns_len, &ident, &id_len) != 0:
            raise ValueError("Invalid Ref format")
        return PyUnicode_DecodeUTF8(ident, <Py_ssize_t>id_len, "strict")

    def nss(self):
        """
        Return the namespace hierarchy as a list.

        Returns
        -------
        list[str]
            Namespace hierarchy split by '-'.

        Raises
        ------
        ValueError
            If the reference format is invalid.
        """
        return self.ns().split('-')

cdef class DmlDbEnv:
    """Daggerml database environment handle."""

    cdef public str path
    cdef public tuple namespaces

    cdef DmlDbHandle* _handle
    cdef int _owns_handle
    cdef object _lock
    cdef int _active_txns

    def __cinit__(self):
        self._handle = NULL
        self._owns_handle = 0
        self._lock = threading.RLock()
        self._active_txns = 0

    property closed:
        def __get__(self) -> bool:
            return self._handle == NULL

    @classmethod
    def _open(cls, str path, list[str] namespaces, bint create_if_missing=False, map_size=None):
        cdef Py_ssize_t i, n = len(namespaces)
        cdef const char **ns_c = NULL
        cdef DmlDbHandle* _handle = NULL
        cdef const char* path_c = PyUnicode_AsUTF8(path)
        if path_c == NULL:
            raise ValueError("Cannot unicode")
        cdef size_t map_size_c = 0
        cdef int rc
        cdef Py_ssize_t ns_size
        cdef object py_ns
        cdef const char *ns_ptr
        cdef char *c_copy
        cdef Py_ssize_t j
        if n == 0:
            raise ValueError("namespaces must be non-empty")
        if map_size is not None:
            if not isinstance(map_size, int):
                raise TypeError("map_size must be int or None")
            if map_size <= 0:
                raise ValueError("map_size must be positive")
            map_size_c = <size_t>map_size

        try:
            ns_c = <const char**>calloc(n, sizeof(const char*))
            if ns_c == NULL:
                raise MemoryError()
            ns_size = 0
            for i in range(n):
                py_ns = <str>namespaces[i]
                ns_ptr = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
                if ns_ptr == NULL:
                    # free any allocated copies
                    for j in range(i):
                        if ns_c[j] != NULL:
                            free(<void*>ns_c[j])
                    free(<void*>ns_c)
                    raise ValueError("Cannot unicode")
                # allocate owned copy for the C library to use
                c_copy = <char*>malloc(ns_size + 1)
                if c_copy == NULL:
                    for j in range(i):
                        if ns_c[j] != NULL:
                            free(<void*>ns_c[j])
                    free(<void*>ns_c)
                    raise MemoryError()
                memcpy(c_copy, ns_ptr, ns_size)
                c_copy[ns_size] = '\0'
                ns_c[i] = <const char*>c_copy
            rc = dml_db_open(
                path_c,
                ns_c,
                <size_t>n,
                1 if create_if_missing else 0,
                map_size_c,
                &_handle
            )
            raise_if_error(rc, "dml_db_open")
        finally:
            if ns_c != NULL:
                # free copied namespace strings then the array
                for i in range(n):
                    if ns_c[i] != NULL:
                        free(<void*>ns_c[i])
                free(<void*>ns_c)

        cdef DmlDbEnv obj = cls.__new__(cls)
        obj.path = path
        obj.namespaces = tuple(namespaces)
        obj._handle = _handle
        obj._owns_handle = 1
        return obj

    @classmethod
    def create(cls, str path, list[str] namespaces, map_size=None):
        return cls._open(path, namespaces, create_if_missing=True, map_size=map_size)

    @classmethod
    def open(cls, str path, list[str] namespaces, map_size=None):
        return cls._open(path, namespaces, create_if_missing=False, map_size=map_size)

    def get_size(self) -> int:
        if self._handle == NULL:
            raise RuntimeError("handle is closed")
        cdef size_t size = 0
        cdef int rc = dml_db_mapsize(&self._handle, &size)
        raise_if_error(rc, "dml_db_mapsize")
        return <int>size

    def resize(self, int new_size):
        if self._handle == NULL:
            raise RuntimeError("handle is closed")
        cdef int rc = dml_db_resize(&self._handle, <size_t>new_size)
        raise_if_error(rc, "dml_db_resize")

    @contextmanager
    def tx(self, readonly=True):
        """Begin a transaction and yield a DmlDbTxn wrapper."""
        cdef DmlDbEnvTxn txn_obj;
        cdef int rc;
        with self._lock:
            txn_obj = DmlDbEnvTxn.__new__(DmlDbEnvTxn)
            if self._handle == NULL:
                raise RuntimeError("handle is closed")
            txn_obj._env = self            # strong ref keeps env alive
            txn_obj._txn = NULL
            txn_obj._closed = 0
            rc = dml_db_txn_begin(&self._handle, 1 if readonly else 0, &txn_obj._txn)
        if rc != 0:
            txn_obj.abort()
            raise_if_error(rc, "dml_db_txn_begin")
        self._active_txns += 1
        try:
            yield txn_obj
        except:
            txn_obj.abort()
            raise
        else:
            txn_obj.commit()
        finally:
            txn_obj._txn = NULL
            txn_obj._closed = 1
            self._active_txns -= 1

    def close(self):
        if self._handle == NULL:
            return
        if self._active_txns != 0:
            raise RuntimeError("cannot close env while transactions are active")
        if self._owns_handle:
            dml_db_close(&self._handle)
            self._handle = NULL

    def __dealloc__(self):
        self.close()


cdef class DmlDbEnvTxn:
    """Transaction wrapper.

    Holds a strong reference to the environment to prevent env close/GC while txn is live.
    """

    cdef DmlDbEnv _env
    cdef DmlDbTxn* _txn
    cdef int _closed

    property closed:
        def __get__(self) -> bool:
            return self._closed or self._txn == NULL

    cdef inline void _check(self) except *:
        if self._env is None or self._env._handle == NULL:
            raise RuntimeError("env is closed")
        if self._txn == NULL or self._closed:
            raise RuntimeError("txn is closed")

    cdef inline void finish(self, success: bool = True) except *:
        cdef int rc;
        with self._env._lock:
            if success:
                self._check()
            if self._txn == NULL:
                raise RuntimeError("txn is closed")
            rc = dml_db_txn_fin(&self._env._handle, self._txn, 1 if success else 0)
            self._txn = NULL
            self._closed = 1
        if success:
            raise_if_error(rc, "dml_db_txn_commit")

    def commit(self):
        self.finish(success=True)

    def abort(self):
        try:
            self.finish(success=False)
        except RuntimeError:
            pass

    # --- Data operations on the txn ---

    def put(self, object value, *, str ns=None, Ref to=None, bint no_overwrite=False, bint raw=False) -> Ref:
        self._check()
        if to is not None and ns is not None:
            raise RuntimeError("ns and to were both not None.")
        if to is None and ns is None:
            raise RuntimeError("Both ns and to were None.")
        cdef int c_no_overwrite = 1 if no_overwrite else 0
        cdef DmlValue *out_ref = NULL
        cdef const char *key_char = NULL
        cdef const char *ns_char = NULL
        cdef Py_ssize_t key_size = 0
        cdef Py_ssize_t ns_size = 0
        cdef int rc
        cdef DmlValue *dv = NULL
        cdef object py_id = None
        cdef object py_ns = None
        cdef const char *data
        cdef Py_ssize_t size

        try:
            if raw:
                if not isinstance(value, str):
                    raise TypeError("raw=True requires value to be a base64 encoded string")
                decoded_bytes = base64.b64decode(value)
                if PyBytes_AsStringAndSize(decoded_bytes, <char **>&data, &size) != 0:
                    raise ValueError("Invalid bytes object")
                dv = dml_value_new_str(data, <size_t>size)
                if dv == NULL:
                    raise MemoryError()
                # keep decoded_bytes alive
                _keep_decoded = decoded_bytes
            else:
                dv = py_to_dml_value(value)
                if dv == NULL:
                    raise MemoryError("py_to_dml_value returned NULL")
            if to is not None:
                ns = to.ns()
                py_id = to.id()
                key_char = PyUnicode_AsUTF8AndSize(py_id, &key_size)
                if key_char == NULL:
                    raise MemoryError("Insufficient memory")
            py_ns = ns
            ns_char = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
            if ns_char == NULL:
                raise MemoryError("Insufficient memory")
            rc = dml_db_put(&self._env._handle, self._txn,
                            ns_char, <size_t>ns_size,
                            key_char, <size_t>key_size,
                            dv, c_no_overwrite, 1 if raw else 0, &out_ref)
        finally:
            if dv != NULL:
                dml_value_free(dv)
        raise_if_error(rc, "dml_db_put")
        if out_ref == NULL:
            raise RuntimeError("dml_db_put succeeded but out_ref is NULL")
        try:
            return dml_value_to_py(out_ref)
        finally:
            dml_value_free(out_ref)

    def get(self, Ref key, bint raw=False) -> object:
        self._check()
        cdef const char *key_char
        cdef const char *ns_char
        cdef Py_ssize_t key_size
        cdef Py_ssize_t ns_size
        cdef DmlValue *out_value = NULL
        cdef int rc
        cdef object py_id = None
        cdef object py_ns = None

        try:
            py_id = key.id()
            key_char = PyUnicode_AsUTF8AndSize(py_id, &key_size)
            if key_char == NULL:
                raise MemoryError("Insufficient memory")
            py_ns = key.ns()
            ns_char = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
            if ns_char == NULL:
                raise MemoryError("Insufficient memory")
            # keep python objects alive while native call uses pointers
            _keep_id = py_id
            _keep_ns = py_ns

            rc = dml_db_get(&self._env._handle, self._txn,
                            ns_char, <size_t>ns_size,
                            key_char, <size_t>key_size,
                            1 if raw else 0,
                            &out_value)
            raise_if_error(rc, f"db.get({key.to})")

            if out_value == NULL:
                raise RuntimeError("dml_db_get returned NULL value")
            if raw:
                raw_bytes = PyBytes_FromStringAndSize(out_value.as.str.data, <Py_ssize_t>out_value.as.str.size)
                return base64.b64encode(raw_bytes).decode('ascii')
            else:
                return dml_value_to_py(out_value)
        finally:
            if out_value != NULL:
                dml_value_free(out_value)

    def delete(self, Ref key) -> None:
        self._check()
        cdef const char *key_char
        cdef const char *ns_char
        cdef Py_ssize_t key_size
        cdef Py_ssize_t ns_size
        cdef int rc
        cdef object py_id = None
        cdef object py_ns = None

        py_id = key.id()
        key_char = PyUnicode_AsUTF8AndSize(py_id, &key_size)
        if key_char == NULL:
            raise MemoryError("Insufficient memory")
        py_ns = key.ns()
        ns_char = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
        if ns_char == NULL:
            raise MemoryError("Insufficient memory")
        # keep python objects alive for duration of native call
        _keep_id = py_id
        _keep_ns = py_ns

        rc = dml_db_del(&self._env._handle, self._txn,
                        ns_char, <size_t>ns_size,
                        key_char, <size_t>key_size)
        raise_if_error(rc, f"db.del({key.to})")

    def exists(self, Ref key) -> bool:
        self._check()
        cdef const char *key_char
        cdef const char *ns_char
        cdef Py_ssize_t key_size
        cdef Py_ssize_t ns_size
        cdef int rc
        cdef int exists = 0

        py_id = key.id()
        key_char = PyUnicode_AsUTF8AndSize(py_id, &key_size)
        if key_char == NULL:
            raise MemoryError("Insufficient memory")
        py_ns = key.ns()
        ns_char = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
        if ns_char == NULL:
            raise MemoryError("Insufficient memory")

        rc = dml_db_exists(&self._env._handle, self._txn,
                           ns_char, <size_t>ns_size,
                           key_char, <size_t>key_size,
                           &exists)
        raise_if_error(rc, f"db.exists({key.to})")
        return bool(exists)

    def iter(self, str ns, start_token=None):
        self._check()
        cdef const char *ns_char
        cdef const char *start_char = NULL
        cdef DmlObjCollection page
        cdef DmlValue **values
        cdef char *keys_ptr
        cdef Py_ssize_t key_len
        cdef Py_ssize_t i
        cdef int rc
        cdef object token = start_token
        cdef object next_token_obj

        ns_char = PyUnicode_AsUTF8(ns)
        if ns_char == NULL:
            raise MemoryError("Insufficient memory")

        while True:
            page.keys = NULL
            page.values = NULL
            page.count = 0
            page.next_token = NULL
            if token is not None:
                start_char = PyUnicode_AsUTF8(token)
                if start_char == NULL:
                    raise MemoryError("Insufficient memory")
            else:
                start_char = NULL

            rc = dml_db_iter_keys(&self._env._handle, self._txn, ns_char, start_char, &page)
            raise_if_error(rc, "dml_db_iter_keys")
            if page.count == 0:
                break

            try:
                values = page.values
                keys_ptr = page.keys
                py_items = []
                for i in range(page.count):
                    key_len = <Py_ssize_t>page.key_lens[i]
                    py_key = PyUnicode_DecodeUTF8(keys_ptr, key_len, "strict")
                    py_value = dml_value_to_py(values[i])
                    # Do NOT free values[i] here — let the native dml_db_free_obj_collection
                    # own and free the native DmlValue memory. Freeing here could double-free
                    # if the native free path also releases these pointers.
                    py_items.append((py_key, py_value))
                    keys_ptr += key_len + 1

                if page.next_token != NULL:
                    next_token_obj = PyUnicode_DecodeUTF8(
                        page.next_token,
                        <Py_ssize_t>strlen(page.next_token),
                        "strict"
                    )
                else:
                    next_token_obj = None
            finally:
                dml_db_free_obj_collection(&page)

            for py_key, py_value in py_items:
                yield Ref(f"{ns}:{py_key}"), py_value

            if next_token_obj is None:
                break
            token = next_token_obj

    def list_orphans(self, list[Ref] start) -> list[Ref]:
        """
        List orphaned references starting from given roots.

        Parameters
        ----------
        start : list[Ref]
            List of Ref strings to start traversal from.

        Returns
        -------
        list[Ref]
            List of orphaned Ref objects.
        """
        self._check()
        cdef Py_ssize_t count = len(start)
        cdef Py_ssize_t i
        cdef Py_ssize_t tmp_len = 0
        cdef const char **refs = NULL
        cdef DmlValue *out_refs = NULL
        cdef int rc
        # C-level temporaries used during building the C string array
        cdef object py_ref
        cdef const char *ref_ptr
        cdef char *c_copy
        cdef Py_ssize_t j
        if count > 0:
            refs = <const char **>calloc(count, sizeof(const char *))
            if refs == NULL:
                raise MemoryError("Insufficient memory")
            for i in range(count):
                py_ref = (<Ref>start[i]).to
                ref_ptr = PyUnicode_AsUTF8AndSize(py_ref, &tmp_len)
                if ref_ptr == NULL:
                    free(<void *>refs)
                    raise MemoryError("Insufficient memory")
                # allocate owned copy for native call
                c_copy = <char*>malloc(tmp_len + 1)
                if c_copy == NULL:
                    for j in range(i):
                        if refs[j] != NULL:
                            free(<void*>refs[j])
                    free(<void *>refs)
                    raise MemoryError("Insufficient memory")
                memcpy(c_copy, ref_ptr, tmp_len)
                c_copy[tmp_len] = '\0'
                refs[i] = <const char*>c_copy
        try:
            rc = dml_db_list_orphans(&self._env._handle, self._txn, refs, <size_t>count, &out_refs)
            raise_if_error(rc, "dml_db_list_orphans")
            if out_refs == NULL:
                return []
            return dml_value_to_py(out_refs)
        finally:
            if refs != NULL:
                # free copies
                for i in range(count):
                    if refs[i] != NULL:
                        free(<void *>refs[i])
                free(<void *>refs)
            if out_refs != NULL:
                dml_value_free(out_refs)
