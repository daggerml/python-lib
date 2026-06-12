#ifndef DAGGERML_DML_CORE2_H
#define DAGGERML_DML_CORE2_H

#include <stddef.h>

#include "dml_value.h"

#define DML_DB_ITER_LIMIT 64

typedef struct DmlDbHandle DmlDbHandle;
typedef struct DmlDbTxnHandle DmlDbTxnHandle;
typedef struct DmlObjCollection {
    char *keys;  // at most DML_DB_ITER_LIMIT keys concatenated together with null terminators
    size_t *key_lens; // per-key lengths to support binary keys containing NUL bytes
    DmlValue **values; // the values corresponding to the keys in the same order.
    size_t count;
    char *next_token;
} DmlObjCollection;

enum {
    DML_DB_OK = 0,
    DML_DB_ERR_HANDLE_INVALID = -1,
    DML_DB_ERR_HANDLE_CLOSED = -2,
    DML_DB_ERR_HANDLE_FORKED = -3,
    DML_DB_ERR_TXN_INVALID = -4,
    DML_DB_ERR_TXN_READONLY = -5,
    DML_DB_ERR_TXN_FORKED = -6,
    DML_DB_ERR_INPUT_INVALID = -7,
    DML_DB_ERR_TYPE_INVALID = -8,
    DML_DB_ERR_PATH_INVALID = -9,
    DML_DB_ERR_REF_INVALID = -10,
    DML_DB_ERR_NAMESPACE_INVALID = -11,
    DML_DB_ERR_NOT_FOUND = -12,
    DML_DB_ERR_KEY_EXISTS = -13,
    DML_DB_ERR_MSGPACK = -14,
    DML_DB_ERR_NOMEM = -15,
    DML_DB_ERR_MAP_FULL = -16,
    DML_DB_ERR_BUSY = -17,
    DML_DB_ERR_LMDB = -18,
    DML_DB_ERR_INTERNAL = -19,
    DML_DB_ERR_ENV_REOPENED = -20
};

// Compute the current on-disk size of the LMDB path.
int dml_db_get_size(const char *path, size_t *out_size);

// Open an LMDB environment handle.
int dml_db_open(
    const char *path,
    const char *const *namespaces,
    size_t namespace_count,
    const int create_if_missing,
    size_t map_size,
    DmlDbHandle **out_handle
);
int dml_db_resize(DmlDbHandle **p_handle, size_t map_size);
int dml_db_close(DmlDbHandle **p_handle);
int dml_db_txn_open(DmlDbHandle **p_handle, const int readonly, DmlDbTxnHandle **out_txn);
int dml_db_txn_close(DmlDbTxnHandle **p_txn, const int commit);

int dml_db_put(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len,
    const DmlValue *value,
    int no_overwrite,
    int raw,
    DmlValue **out_ref
);
int dml_db_get(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len,
    int raw,
    DmlValue **out_value
);
int dml_db_del(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len
);
int dml_db_exists(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len,
    int *out_exists
);
int dml_db_iter_keys(
    struct DmlDbTxnHandle **p_txn,
    const char *ns,
    const char *start_token,
    DmlObjCollection *out_page
);
void dml_db_free_obj_collection(DmlObjCollection *page);
int dml_db_list_orphans(
    struct DmlDbTxnHandle **p_txn,
    const char *const *start_refs,
    size_t start_refs_count,
    DmlValue **out_refs
);

#endif
