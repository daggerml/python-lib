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
from contextlib import contextmanager
from libc.stdlib cimport malloc, free, calloc
from libc.string cimport strlen, memcpy

from cpython.float cimport PyFloat_AsDouble
from cpython.long cimport PyLong_AsLongLongAndOverflow
from cpython.unicode cimport PyUnicode_AsUTF8, PyUnicode_AsUTF8AndSize, PyUnicode_DecodeUTF8
from cpython.exc cimport PyErr_Clear, PyErr_Occurred


logger = logging.getLogger(__name__)
MAX_STRING_BYTES = 1024 * 1024
MAX_COLLECTION_LEN = 100000

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
    int DML_DB_ERR_TXN_INVALID
    int DML_DB_ERR_TXN_READONLY
    int DML_DB_ERR_TXN_FORKED
    int DML_DB_ERR_INPUT_INVALID
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
    int DML_DB_ERR_REGISTRY_FULL
    int DML_DB_ERR_MAP_SIZE_MAX

    ctypedef struct DmlObjCollection:
        char *keys
        size_t *key_lens
        DmlValue **values
        size_t count
        char *next_token

    ctypedef struct DmlDbHandle:
        pass
    int dml_db_txn_open(
        const char *path,
        const char *const *namespaces,
        size_t namespace_count,
        const int readonly,
        const int create_if_missing,
        size_t map_size,
        DmlDbHandle **out_handle,
    ) nogil
    int dml_db_txn_close(DmlDbHandle **p_handle, const int commit) nogil
    int dml_db_resize(
        const char *path,
        const char *const *namespaces,
        size_t namespace_count,
        const int create_if_missing,
        size_t headroom,
        size_t max_map_size,
        size_t *out_current_map_size,
    ) nogil

    int dml_db_put(
        DmlDbHandle **p_txn,
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
        DmlDbHandle **p_txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len,
        int raw,
        DmlValue **out_value
    ) nogil
    int dml_db_del(
        DmlDbHandle **p_txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len
    ) nogil
    int dml_db_exists(
        DmlDbHandle **p_txn,
        const char *ns,
        size_t ns_len,
        const char *key,
        size_t key_len,
        int *out_exists
    ) nogil

    int dml_db_iter_keys(
        DmlDbHandle **p_txn,
        const char *ns,
        const char *start_token,
        DmlObjCollection *out_page
    ) nogil
    void dml_db_free_obj_collection(DmlObjCollection *page) nogil
    int dml_db_list_orphans(
        DmlDbHandle **p_txn,
        const char *const *start_refs,
        size_t start_refs_count,
        const char *const *missing_commit_refs,
        size_t missing_commit_refs_count,
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
                    raise MemoryError("Failed to convert list item")
                if dml_value_list_set(result, <size_t>i, child) != 0:
                    dml_value_free(child)
                    raise DmlDbInvalidTypeError(
                        f"invalid list entry for db storage: {obj[i]!r} "
                        "(reason: unsupported type)"
                    )
            return result
        except Exception:
            dml_value_free(result)
            raise
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

class DmlDbRegistryFullError(DmlDbError):
    """
    Process-local DB registry capacity was exceeded.
    """
    pass

cdef inline object raise_if_error(int rc, str context):
    cls = RuntimeError
    if rc == 0:
        return
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
    elif rc == DML_DB_ERR_REGISTRY_FULL:
        cls = DmlDbRegistryFullError
        prefix = "database registry is full"
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
            Reference string in `namespace:id` form.

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
            raise ValueError(f"Ref.ns() encountered invalid Ref format for {self.to!r}")
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
            raise ValueError(f"Ref.id() encountered invalid Ref format for {self.to!r}")
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

cdef class DmlDb:
    """Daggerml database config holder backed by shared registry leases."""

    cdef public str path
    cdef public tuple namespaces
    cdef public size_t map_size_headroom
    cdef public size_t max_map_size
    # private C-level path and namespaces
    cdef const char* _path_c
    cdef const char** _namespaces_c
    cdef size_t _namespace_count
    def __cinit__(self):
        self.path = ""
        self.namespaces = ()
        self._path_c = NULL
        self._namespaces_c = NULL
        self._namespace_count = 0

    def __init__(self, str path, list[str] namespaces, size_t map_size_headroom, size_t max_map_size):
        cdef Py_ssize_t i, n = len(namespaces)
        cdef const char **ns_c = NULL
        cdef const char *path_src = NULL
        cdef Py_ssize_t path_size = 0
        cdef Py_ssize_t ns_size
        cdef object py_ns
        cdef const char *ns_ptr
        cdef char *c_copy
        cdef Py_ssize_t j

        if n == 0:
            raise ValueError("namespaces must be non-empty")
        if map_size_headroom <= 0:
            raise ValueError("map_size_headroom must be positive")
        if max_map_size <= 0:
            raise ValueError("max_map_size must be positive")
        path_src = PyUnicode_AsUTF8AndSize(path, &path_size)
        if path_src == NULL:
            raise ValueError("Cannot unicode")
        try:
            self._path_c = <const char*>malloc(path_size + 1)
            if self._path_c == NULL:
                raise MemoryError()
            memcpy(<void*>self._path_c, path_src, path_size)
            (<char*>self._path_c)[path_size] = '\0'
            ns_c = <const char**>calloc(n, sizeof(const char*))
            if ns_c == NULL:
                raise MemoryError()
            ns_size = 0
            for i in range(n):
                py_ns = <str>namespaces[i]
                ns_ptr = PyUnicode_AsUTF8AndSize(py_ns, &ns_size)
                if ns_ptr == NULL:
                    for j in range(i):
                        if ns_c[j] != NULL:
                            free(<void*>ns_c[j])
                    free(<void*>ns_c)
                    raise ValueError("Cannot unicode")
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
            self._namespaces_c = ns_c
            self._namespace_count = <size_t>n
            ns_c = NULL
        except Exception:
            if ns_c != NULL:
                for i in range(n):
                    if ns_c[i] != NULL:
                        free(<void*>ns_c[i])
                free(<void*>ns_c)
            if self._namespaces_c != NULL:
                for i in range(n):
                    if self._namespaces_c[i] != NULL:
                        free(<void*>self._namespaces_c[i])
                free(<void*>self._namespaces_c)
                self._namespaces_c = NULL
            self._namespace_count = 0
            if self._path_c != NULL:
                free(<void*>self._path_c)
                self._path_c = NULL
            raise
        self.path = path
        self.namespaces = tuple(namespaces)
        self.map_size_headroom = <size_t>map_size_headroom
        self.max_map_size = <size_t>max_map_size

    def resize(self, bint create_if_missing=False) -> None:
        cdef int rc
        cdef size_t current_map_size = 0

        with nogil:
            rc = dml_db_resize(
                self._path_c,
                self._namespaces_c,
                self._namespace_count,
                1 if create_if_missing else 0,
                self.map_size_headroom,
                self.max_map_size,
                &current_map_size,
            )
        if rc == DML_DB_ERR_MAP_SIZE_MAX:
            raise DmlDbMapFullError(
                f"database map is full at {current_map_size} bytes for {self.path}; "
                f"configured maximum is {self.max_map_size} bytes"
            )
        raise_if_error(rc, "dml_db_resize")

    cdef DmlDbHandle* _txn_open(self, bint readonly, bint create_if_missing, size_t map_size) except NULL:
        cdef DmlDbHandle *handle = NULL
        cdef int rc

        with nogil:
            rc = dml_db_txn_open(
                self._path_c,
                self._namespaces_c,
                self._namespace_count,
                1 if readonly else 0,
                1 if create_if_missing else 0,
                map_size,
                &handle,
            )
        raise_if_error(rc, "dml_db_txn_open")
        return handle

    def __dealloc__(self):
        cdef size_t i

        if self._namespaces_c != NULL:
            for i in range(self._namespace_count):
                if self._namespaces_c[i] != NULL:
                    free(<void*>self._namespaces_c[i])
            free(<void*>self._namespaces_c)
            self._namespaces_c = NULL
        if self._path_c != NULL:
            free(<void*>self._path_c)
            self._path_c = NULL
        self._namespace_count = 0

    @contextmanager
    def tx(self, map_size=None, bint readonly=True, bint create_if_missing=False):
        cdef int rc = 0
        cdef int success = 1
        cdef size_t map_size_value = 0 if map_size is None else <size_t>map_size
        if map_size is None and create_if_missing and not os.path.exists(os.path.join(self.path, "data.mdb")):
            map_size_value = self.map_size_headroom
            if map_size_value > self.max_map_size:
                map_size_value = self.max_map_size
        cdef DmlDbTxn txn = DmlDbTxn(
            self,
            readonly=readonly,
            create_if_missing=create_if_missing,
            map_size=map_size_value,
        )
        try:
            yield txn
        except Exception:
            success = 0
            raise
        finally:
            with nogil:
                rc = dml_db_txn_close(&txn._handle, 1 if success else 0)
            txn._handle = NULL
            del txn
        if success:
            raise_if_error(rc, "dml_db_txn_close")

    def write_with_growth(self, fn, bint create_if_missing=False):
        while True:
            try:
                with self.tx(
                    readonly=False,
                    create_if_missing=create_if_missing,
                ) as txn:
                    return fn(txn)
            except DmlDbMapFullError:
                self.resize(create_if_missing=create_if_missing)

cdef class DmlDbTxn:
    """Daggerml database transaction handle."""

    cdef DmlDbHandle* _handle

    def __cinit__(self):
        self._handle = NULL

    def __init__(self, DmlDb db, bint readonly, bint create_if_missing=False, size_t map_size=0):
        self._handle = db._txn_open(readonly, create_if_missing, map_size)

    # --- Data operations on the handle ---

    def put(self, object value, *, str ns=None, Ref to=None, bint no_overwrite=False) -> Ref:
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
        try:
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
            rc = dml_db_put(&self._handle,
                            ns_char, <size_t>ns_size,
                            key_char, <size_t>key_size,
                            dv, c_no_overwrite, 0, &out_ref)
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

    def get(self, Ref key) -> object:
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
            _keep_id = py_id
            _keep_ns = py_ns

            rc = dml_db_get(&self._handle,
                            ns_char, <size_t>ns_size,
                            key_char, <size_t>key_size,
                            0,
                            &out_value)
            raise_if_error(rc, f"db.get({key.to})")

            if out_value == NULL:
                raise RuntimeError("dml_db_get returned NULL value")
            return dml_value_to_py(out_value)
        finally:
            if out_value != NULL:
                dml_value_free(out_value)

    def delete(self, Ref key) -> None:
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
        _keep_id = py_id
        _keep_ns = py_ns

        rc = dml_db_del(&self._handle,
                        ns_char, <size_t>ns_size,
                        key_char, <size_t>key_size)
        raise_if_error(rc, f"db.del({key.to})")

    def exists(self, Ref key) -> bool:
        cdef const char *key_char
        cdef const char *ns_char
        cdef Py_ssize_t key_size
        cdef Py_ssize_t ns_size
        cdef int rc
        cdef int exists = 0
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
        _keep_id = py_id
        _keep_ns = py_ns

        rc = dml_db_exists(&self._handle,
                           ns_char, <size_t>ns_size,
                           key_char, <size_t>key_size,
                           &exists)
        raise_if_error(rc, f"db.exists({key.to})")
        return bool(exists)

    def iter(self, str ns, start_token=None):
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

            rc = dml_db_iter_keys(&self._handle, ns_char, start_char, &page)
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

    def list_orphans(self, list[Ref] start, list[Ref] missing_commit_refs=None) -> list[Ref]:
        cdef Py_ssize_t count = len(start)
        cdef Py_ssize_t missing_count = 0 if missing_commit_refs is None else len(missing_commit_refs)
        cdef Py_ssize_t i
        cdef Py_ssize_t tmp_len = 0
        cdef const char **refs = NULL
        cdef const char **missing_refs = NULL
        cdef DmlValue *out_refs = NULL
        cdef int rc
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
                    for j in range(i):
                        if refs[j] != NULL:
                            free(<void*>refs[j])
                    free(<void *>refs)
                    raise MemoryError("Insufficient memory")
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
        if missing_count > 0:
            missing_refs = <const char **>calloc(missing_count, sizeof(const char *))
            if missing_refs == NULL:
                if refs != NULL:
                    for j in range(count):
                        if refs[j] != NULL:
                            free(<void*>refs[j])
                    free(<void *>refs)
                raise MemoryError("Insufficient memory")
            for i in range(missing_count):
                py_ref = (<Ref>missing_commit_refs[i]).to
                ref_ptr = PyUnicode_AsUTF8AndSize(py_ref, &tmp_len)
                if ref_ptr == NULL:
                    for j in range(i):
                        if missing_refs[j] != NULL:
                            free(<void*>missing_refs[j])
                    free(<void *>missing_refs)
                    if refs != NULL:
                        for j in range(count):
                            if refs[j] != NULL:
                                free(<void*>refs[j])
                        free(<void *>refs)
                    raise MemoryError("Insufficient memory")
                c_copy = <char*>malloc(tmp_len + 1)
                if c_copy == NULL:
                    for j in range(i):
                        if missing_refs[j] != NULL:
                            free(<void*>missing_refs[j])
                    free(<void *>missing_refs)
                    if refs != NULL:
                        for j in range(count):
                            if refs[j] != NULL:
                                free(<void*>refs[j])
                        free(<void *>refs)
                    raise MemoryError("Insufficient memory")
                memcpy(c_copy, ref_ptr, tmp_len)
                c_copy[tmp_len] = '\0'
                missing_refs[i] = <const char*>c_copy
        try:
            rc = dml_db_list_orphans(
                &self._handle,
                refs,
                <size_t>count,
                missing_refs,
                <size_t>missing_count,
                &out_refs,
            )
            raise_if_error(rc, "dml_db_list_orphans")
            if out_refs == NULL:
                return []
            return dml_value_to_py(out_refs)
        finally:
            if refs != NULL:
                for i in range(count):
                    if refs[i] != NULL:
                        free(<void *>refs[i])
                free(<void *>refs)
            if missing_refs != NULL:
                for i in range(missing_count):
                    if missing_refs[i] != NULL:
                        free(<void *>missing_refs[i])
                free(<void *>missing_refs)
            if out_refs != NULL:
                dml_value_free(out_refs)
