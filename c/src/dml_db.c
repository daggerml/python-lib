#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include "../third_party/lmdb/libraries/liblmdb/lmdb.h"
#include "../third_party/msgpack/include/msgpack.h"
#include "../include/dml_db.h"
#include "../include/dml_hash.h"
#include "../include/dml_msgpack.h"

struct DmlDbHandle {
    MDB_env *env;
    MDB_dbi *dbis;
    char **namespaces;
    char *path;
    size_t namespace_count;
    pid_t owner_pid;
};

struct DmlDbTxnHandle {
    DmlDbHandle *db;
    MDB_txn *txn;
    pthread_t owner_thread;
    bool readonly;
};

typedef struct {
    char *key;
    size_t key_len;
    void *value;
    size_t value_len;
} DmlDumpEntry;

typedef struct {
    DmlDumpEntry *entries;
    size_t count;
    size_t capacity;
} DmlDumpList;

static int dml_db_reopen(struct DmlDbHandle **p_handle) {
    struct DmlDbHandle *old_handle;
    DmlDbHandle *new_handle = NULL;
    int forked;
    int rc;

    if (p_handle == NULL || *p_handle == NULL) return DML_DB_ERR_HANDLE_INVALID;
    old_handle = *p_handle;
    forked = old_handle->owner_pid != getpid();
    rc = dml_db_open(
        old_handle->path,
        (const char *const *)old_handle->namespaces,
        old_handle->namespace_count,
        0,
        0,
        &new_handle
    );
    if (rc != 0) return rc;
    if (!forked) {
        dml_db_close(p_handle);
    }
    *p_handle = new_handle;
    return 0;
}

static int dml_map_lmdb_rc(int rc) {
    if (rc == MDB_MAP_FULL) {
        return DML_DB_ERR_MAP_FULL;
    }
    if (rc == ENOMEM) {
        return DML_DB_ERR_NOMEM;
    }
    if (rc == ENOENT || rc == ENOTDIR || rc == EACCES) {
        return DML_DB_ERR_PATH_INVALID;
    }
    if (rc == MDB_KEYEXIST) {
        return DML_DB_ERR_KEY_EXISTS;
    }
    if (rc == EBUSY || rc == EAGAIN) {
        return DML_DB_ERR_BUSY;
    }
    return DML_DB_ERR_LMDB;
}

int dml_db_get_size(const char *path, size_t *out_size) {
    struct stat st;
    size_t total = 0;

    if (path == NULL || out_size == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    if (stat(path, &st) != 0) {
        if (errno == ENOENT) {
            *out_size = 0;
            return 0;
        }
        return dml_map_lmdb_rc(errno);
    }
    if (S_ISDIR(st.st_mode)) {
        DIR *dir = opendir(path);
        struct dirent *entry;

        if (dir == NULL) {
            return dml_map_lmdb_rc(errno);
        }
        while ((entry = readdir(dir)) != NULL) {
            char *child_path = NULL;
            struct stat child_st;
            size_t path_len;
            size_t name_len;

            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            path_len = strlen(path);
            name_len = strlen(entry->d_name);
            child_path = (char *)malloc(path_len + 1 + name_len + 1);
            if (child_path == NULL) {
                closedir(dir);
                return DML_DB_ERR_NOMEM;
            }
            memcpy(child_path, path, path_len);
            child_path[path_len] = '/';
            memcpy(child_path + path_len + 1, entry->d_name, name_len + 1);
            if (stat(child_path, &child_st) != 0) {
                int stat_errno = errno;

                free(child_path);
                closedir(dir);
                return dml_map_lmdb_rc(stat_errno);
            }
            free(child_path);
            if (S_ISREG(child_st.st_mode)) {
                total += (size_t)child_st.st_size;
            }
        }
        closedir(dir);
    } else if (S_ISREG(st.st_mode)) {
        total += (size_t)st.st_size;
    }
    *out_size = total;
    return 0;
}

static void dml_dump_list_free(DmlDumpList *list) {
    if (list == NULL) return;
    for (size_t i = 0; i < list->count; i++) {
        free(list->entries[i].key);
        if (list->entries[i].value != NULL) {
            dml_value_free((DmlValue *)list->entries[i].value);
        }
    }
    free(list->entries);
    list->entries = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int dml_dump_list_find(const DmlDumpList *list, const char *key, size_t key_len) {
    /* TODO: Reachability/orphan traversal uses this as a seen check, so this linear scan can become costly on large graphs. */
    for (size_t i = 0; i < list->count; i++) {
        if (list->entries[i].key_len == key_len &&
            memcmp(list->entries[i].key, key, key_len) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int dml_dump_list_add(DmlDumpList *list, const char *key, size_t key_len, DmlValue *value) {
    if (list->count == list->capacity) {
        size_t next = list->capacity == 0 ? 8 : list->capacity * 2;
        DmlDumpEntry *next_entries = (DmlDumpEntry *)realloc(list->entries, next * sizeof(*next_entries));
        if (next_entries == NULL) {
            return DML_DB_ERR_NOMEM;
        }
        list->entries = next_entries;
        list->capacity = next;
    }
    char *key_copy = (char *)malloc(key_len);
    if (key_copy == NULL) {
        return DML_DB_ERR_NOMEM;
    }
    memcpy(key_copy, key, key_len);
    list->entries[list->count].key = key_copy;
    list->entries[list->count].key_len = key_len;
    list->entries[list->count].value = value;
    list->entries[list->count].value_len = 0;
    list->count += 1;
    return 0;
}

static int dml_dump_visit_value(
    DmlDbTxnHandle **p_txn,
    DmlDumpList *list,
    const DmlValue *value
);

static int dml_dump_add_ref(
    DmlDbTxnHandle **p_txn,
    DmlDumpList *list,
    const char *key,
    size_t key_len
) {
    const char *ns = NULL;
    const char *ident = NULL;
    size_t ns_len = 0;
    size_t id_len = 0;
    DmlValue *value = NULL;
    int rc;

    if (dml_dump_list_find(list, key, key_len) >= 0) {
        return 0;
    }
    if (dml_ref_split(key, key_len, &ns, &ns_len, &ident, &id_len) != 0) {
        return DML_DB_ERR_REF_INVALID;
    }
    rc = dml_db_get(p_txn, ns, ns_len, ident, id_len, 0, &value);
    if (rc != 0) {
        return rc;
    }
    rc = dml_dump_list_add(list, key, key_len, value);
    if (rc != 0) {
        dml_value_free(value);
        return rc;
    }
    rc = dml_dump_visit_value(p_txn, list, value);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

static int dml_dump_visit_value(
    DmlDbTxnHandle **p_txn,
    DmlDumpList *list,
    const DmlValue *value
) {
    if (value == NULL) return 0;
    switch (value->type) {
    case DML_VALUE_REF:
        return dml_dump_add_ref(p_txn, list, value->as.ref.data, value->as.ref.size);
    case DML_VALUE_LIST:
        for (size_t i = 0; i < value->as.list.count; i++) {
            int rc = dml_dump_visit_value(p_txn, list, value->as.list.items[i]);
            if (rc != 0) return rc;
        }
        return 0;
    case DML_VALUE_MAP:
        for (size_t i = 0; i < value->as.map.count; i++) {
            int rc = dml_dump_visit_value(p_txn, list, value->as.map.entries[i].value);
            if (rc != 0) return rc;
        }
        return 0;
    default:
        return 0;
    }
}

static int dml_db_validate(struct DmlDbHandle **p_handle, const int reopen) {
    if (p_handle == NULL || *p_handle == NULL) return DML_DB_ERR_HANDLE_INVALID;
    MDB_stat st;
    int rc;
    struct DmlDbHandle *handle = *p_handle;
    if (handle->owner_pid != getpid()) {
        if (reopen) {
            rc = dml_db_reopen(p_handle);
            if (rc != 0) return rc;
            handle = *p_handle;
        } else {
            return DML_DB_ERR_HANDLE_FORKED;
        }
    }
    if (handle->env == NULL) {
        if (reopen) {
            rc = dml_db_reopen(p_handle);
            if (rc != 0) return rc;
            handle = *p_handle;
        } else {
            return DML_DB_ERR_HANDLE_CLOSED;
        }
    }
    if (handle->dbis == NULL) return DML_DB_ERR_HANDLE_INVALID;
    if (handle->namespace_count == 0) return DML_DB_ERR_HANDLE_INVALID;
    rc = mdb_env_stat(handle->env, &st);
    if (rc != MDB_SUCCESS && reopen) {
        rc = dml_db_reopen(p_handle);
        if (rc != 0) return rc;
        handle = *p_handle;
        rc = mdb_env_stat(handle->env, &st);
    }
    if (rc != MDB_SUCCESS) {
        return dml_map_lmdb_rc(rc);
    }
    return 0;
}

static int dml_db_validate_txn(struct DmlDbTxnHandle **p_txn, const int reopen) {
    int rc;
    if (p_txn == NULL || *p_txn == NULL) {
        return DML_DB_ERR_TXN_INVALID;
    }
    rc = dml_db_validate(&(*p_txn)->db, reopen);
    if (rc != 0) {
        if (rc == DML_DB_ERR_HANDLE_FORKED) {
            return DML_DB_ERR_TXN_FORKED;
        }
        return rc;
    }
    if ((*p_txn)->txn == NULL) {
        return DML_DB_ERR_TXN_INVALID;
    }
    if (pthread_self() != (*p_txn)->owner_thread) {
        return DML_DB_ERR_TXN_FORKED;
    }
    return 0;
}

// dml_db_open opens the environment handle.
int dml_db_open(
    const char *path,
    const char *const *namespaces,
    size_t namespace_count,
    const int create_if_missing,
    size_t map_size,
    DmlDbHandle **out_handle
) {
    DmlDbHandle *handle = NULL;
    MDB_txn *setup_txn = NULL;
    int rc;
    if (path == NULL || out_handle == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    if (namespace_count == 0 || namespaces == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    handle = (DmlDbHandle *)calloc(1, sizeof(DmlDbHandle));
    if (handle == NULL) {
        return DML_DB_ERR_NOMEM;
    }
    rc = mdb_env_create(&handle->env);
    if (rc != MDB_SUCCESS) {
        dml_db_close(&handle);
        return dml_map_lmdb_rc(rc);
    }
    rc = mdb_env_set_maxdbs(handle->env, (unsigned int)namespace_count);
    if (rc != MDB_SUCCESS) {
        dml_db_close(&handle);
        return dml_map_lmdb_rc(rc);
    }
    if (map_size > 0) {
        rc = mdb_env_set_mapsize(handle->env, map_size);
        if (rc != MDB_SUCCESS) {
            dml_db_close(&handle);
            return dml_map_lmdb_rc(rc);
        }
    }
    rc = mdb_env_open(handle->env, path, create_if_missing ? MDB_CREATE : 0, 0664);
    if (rc != MDB_SUCCESS) {
        dml_db_close(&handle);
        return dml_map_lmdb_rc(rc);
    }
    handle->owner_pid = getpid();
    // Copy namespaces
    if (namespaces != NULL && namespace_count > 0) {
        size_t i;
        handle->namespaces = (char **)calloc(namespace_count, sizeof(char *));
        if (handle->namespaces == NULL) {
            dml_db_close(&handle);
            return DML_DB_ERR_NOMEM;
        }
        handle->namespace_count = namespace_count;
        for (i = 0; i < namespace_count; i++) {
            if (namespaces[i] == NULL) {
                dml_db_close(&handle);
                return DML_DB_ERR_INPUT_INVALID;
            }
            size_t len = strlen(namespaces[i]);
            handle->namespaces[i] = (char *)malloc(len + 1);
            if (handle->namespaces[i] == NULL) {
                dml_db_close(&handle);
                return DML_DB_ERR_NOMEM;
            }
            memcpy(handle->namespaces[i], namespaces[i], len);
            handle->namespaces[i][len] = '\0';
        }
    }
    handle->path = strdup(path);
    if (handle->path == NULL) {
        dml_db_close(&handle);
        return DML_DB_ERR_NOMEM;
    }
    handle->dbis = (MDB_dbi *)calloc(namespace_count, sizeof(MDB_dbi));
    if (handle->dbis == NULL) {
        dml_db_close(&handle);
        return DML_DB_ERR_NOMEM;
    }
    rc = mdb_txn_begin(handle->env, NULL, 0, &setup_txn);
    if (rc != MDB_SUCCESS) {
        dml_db_close(&handle);
        return dml_map_lmdb_rc(rc);
    }
    for (size_t i = 0; i < namespace_count; i++) {
        rc = mdb_dbi_open(setup_txn, handle->namespaces[i], MDB_CREATE, &handle->dbis[i]);
        if (rc != MDB_SUCCESS) {
            mdb_txn_abort(setup_txn);
            dml_db_close(&handle);
            return dml_map_lmdb_rc(rc);
        }
    }
    rc = mdb_txn_commit(setup_txn);
    if (rc != MDB_SUCCESS) {
        dml_db_close(&handle);
        return dml_map_lmdb_rc(rc);
    }
    *out_handle = handle;
    return 0;
}

int dml_db_close(struct DmlDbHandle **p_handle) {
    if (p_handle == NULL || *p_handle == NULL) return 0;
    struct DmlDbHandle *handle = *p_handle;
    *p_handle = NULL;
    if (handle->dbis != NULL) {
        free(handle->dbis);
        handle->dbis = NULL;
    }
    if (handle->namespaces != NULL) {
        for (size_t i = 0; i < handle->namespace_count; i++) {
            free(handle->namespaces[i]);     // ok if NULL
        }
        free(handle->namespaces);
        handle->namespaces = NULL;
    }
    if (handle->path != NULL) {
        free(handle->path);
        handle->path = NULL;
    }
    if (handle->env != NULL) mdb_env_close(handle->env);
    free(handle);
    return 0;
}

int dml_db_resize(DmlDbHandle **p_handle, size_t map_size) {
    int rc;

    if (map_size == 0) return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate(p_handle, 1);
    if (rc != 0) return rc;
    rc = mdb_env_set_mapsize((*p_handle)->env, map_size);
    if (rc != MDB_SUCCESS) return dml_map_lmdb_rc(rc);
    return 0;
}

int dml_db_txn_open(DmlDbHandle **p_handle, const int readonly, DmlDbTxnHandle **out_txn) {
    DmlDbTxnHandle *txn_handle = NULL;
    MDB_txn *txn = NULL;
    int rc;

    if (out_txn == NULL) return DML_DB_ERR_INPUT_INVALID;
    *out_txn = NULL;
    rc = dml_db_validate(p_handle, 1);
    if (rc != 0) return rc;
    txn_handle = (DmlDbTxnHandle *)calloc(1, sizeof(DmlDbTxnHandle));
    if (txn_handle == NULL) return DML_DB_ERR_NOMEM;
    rc = mdb_txn_begin((*p_handle)->env, NULL, readonly ? MDB_RDONLY : 0, &txn);
    if (rc != MDB_SUCCESS) {
        free(txn_handle);
        return dml_map_lmdb_rc(rc);
    }
    txn_handle->db = *p_handle;
    txn_handle->txn = txn;
    txn_handle->owner_thread = pthread_self();
    txn_handle->readonly = readonly ? true : false;
    *out_txn = txn_handle;
    return 0;
}

int dml_db_txn_close(DmlDbTxnHandle **p_txn, const int commit) {
    int rc = 0;
    if (p_txn == NULL || *p_txn == NULL) return 0;
    DmlDbTxnHandle *txn_handle = *p_txn;
    *p_txn = NULL;
    if (txn_handle->txn != NULL) {
        if (txn_handle->readonly || !commit) {
            mdb_txn_abort(txn_handle->txn);
        } else {
            rc = mdb_txn_commit(txn_handle->txn);
            if (rc != MDB_SUCCESS) {
                rc = dml_map_lmdb_rc(rc);
            } else {
                rc = 0;
            }
        }
        txn_handle->txn = NULL;
    }
    free(txn_handle);
    return rc;
}

// io
int dml_ns_dbi_lookup(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    MDB_dbi *out_dbi
) {
    // look for namespace in handle and return the cached DBI
    size_t i;
    int rc;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    if (ns == NULL || ns_len == 0 || out_dbi == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    DmlDbHandle *handle = (*p_txn)->db;
    for (i = 0; i < handle->namespace_count; i++) {
        if (strlen(handle->namespaces[i]) == ns_len &&
            memcmp(handle->namespaces[i], ns, ns_len) == 0) {
            // found
            *out_dbi = handle->dbis[i];
            return 0;
        }
    }
    return DML_DB_ERR_NAMESPACE_INVALID;
}
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
) {
    int rc = 0;
    MDB_dbi dbi;
    DmlMsgpackBuffer buffer = {0};

    char *owned_key = NULL;          // <— if we compute it, we own it
    size_t owned_key_len = 0;
    MDB_val db_value = {0};

    if (ns == NULL || ns_len == 0)   return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    if ((*p_txn)->readonly) { rc = DML_DB_ERR_TXN_READONLY; goto cleanup; }
    if (raw) {
        // Raw mode: value should be a string DmlValue containing raw bytes
        if (value->type != DML_VALUE_STR) {
            rc = DML_DB_ERR_INPUT_INVALID;
            goto cleanup;
        }
        // Store raw bytes directly - no msgpack packing
        db_value.mv_data = value->as.str.data;
        db_value.mv_size = value->as.str.size;
    } else {
        rc = dml_msgpack_pack(value, &buffer);
        if (rc != 0) { rc = DML_DB_ERR_MSGPACK; goto cleanup; }
        db_value.mv_data = buffer.data;
        db_value.mv_size = buffer.size;
    }
    if (key == NULL || key_len == 0) {
        char hex[65];
        if (raw) {
            // For raw mode, hash the raw data directly
            if (dml_hash_sha256_hex(db_value.mv_data, db_value.mv_size, hex) != 0) {
                rc = DML_DB_ERR_INTERNAL;
                goto cleanup;
            }
        } else {
            // For normal mode, hash the buffer data
            if (dml_hash_sha256_hex(buffer.data, buffer.size, hex) != 0) {
                rc = DML_DB_ERR_INTERNAL;
                goto cleanup;
            }
        }
        owned_key_len = strlen(hex);
        owned_key = (char *)malloc(owned_key_len);
        if (!owned_key) { rc = DML_DB_ERR_NOMEM; goto cleanup; }
        memcpy(owned_key, hex, owned_key_len);
        key = owned_key;
        key_len = owned_key_len;
    }
    rc = dml_ns_dbi_lookup(p_txn, ns, ns_len, &dbi);
    if (rc != 0) { goto cleanup; }
    {
        MDB_val db_key = { .mv_size = key_len, .mv_data = (void *)key };
        unsigned int flags = no_overwrite ? MDB_NOOVERWRITE : 0;

        rc = mdb_put((*p_txn)->txn, dbi, &db_key, &db_value, flags);
        if (rc == MDB_KEYEXIST && no_overwrite) {
            rc = 0;
        }
        if (rc != MDB_SUCCESS) {
            rc = dml_map_lmdb_rc(rc);
            goto cleanup;
        }
    }
    if (out_ref != NULL) {
        size_t ref_len = ns_len + 1 + key_len;
        char *ref_data = (char *)malloc(ref_len);
        if (!ref_data) { rc = DML_DB_ERR_NOMEM; goto cleanup; }

        memcpy(ref_data, ns, ns_len);
        ref_data[ns_len] = ':';
        memcpy(ref_data + ns_len + 1, key, key_len);

        DmlValue *ref_value = dml_value_new_ref(ref_data, ref_len);
        free(ref_data);

        if (!ref_value) { rc = DML_DB_ERR_NOMEM; goto cleanup; }
        *out_ref = ref_value;
    }
cleanup:
    if (buffer.data) dml_msgpack_free_buffer(buffer.data);
    free(owned_key);
    return rc;
}
int dml_db_get(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len,
    int raw,
    DmlValue **out_value
) {
    int rc = 0;
    MDB_dbi dbi;
    MDB_val db_key;
    MDB_val db_value;
    if (key == NULL || key_len == 0) return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    // lookup namespace
    rc = dml_ns_dbi_lookup(p_txn, ns, ns_len, &dbi);
    if (rc != 0) return rc;
    // get value
    db_key.mv_size = key_len;
    db_key.mv_data = (void *)key;
    rc = mdb_get((*p_txn)->txn, dbi, &db_key, &db_value);
    if (rc == MDB_NOTFOUND) {
        return DML_DB_ERR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        return dml_map_lmdb_rc(rc);
    }
    if (raw) {
        // Return raw bytes as a string DmlValue
        *out_value = dml_value_new_str(db_value.mv_data, db_value.mv_size);
        if (*out_value == NULL) {
            return DML_DB_ERR_NOMEM;
        }
    } else {
        rc = dml_msgpack_unpack(db_value.mv_data, db_value.mv_size, out_value);
        if (rc != 0) {
            return DML_DB_ERR_MSGPACK;
        }
    }
    return 0;
}

int dml_db_del(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len
) {
    int rc = 0;
    MDB_dbi dbi;
    MDB_val db_key;
    if (key == NULL || key_len == 0) return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    if ((*p_txn)->readonly) return DML_DB_ERR_TXN_READONLY;

    rc = dml_ns_dbi_lookup(p_txn, ns, ns_len, &dbi);
    if (rc != 0) return rc;

    db_key.mv_size = key_len;
    db_key.mv_data = (void *)key;
    rc = mdb_del((*p_txn)->txn, dbi, &db_key, NULL);
    if (rc == MDB_NOTFOUND) return DML_DB_ERR_NOT_FOUND;
    if (rc != MDB_SUCCESS) return dml_map_lmdb_rc(rc);
    return 0;
}

int dml_db_exists(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    size_t ns_len,
    const char *key,
    size_t key_len,
    int *out_exists
) {
    int rc = 0;
    MDB_dbi dbi;
    MDB_val db_key;
    MDB_val db_value;
    if (out_exists == NULL) return DML_DB_ERR_INPUT_INVALID;
    *out_exists = 0;
    if (key == NULL || key_len == 0) return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    rc = dml_ns_dbi_lookup(p_txn, ns, ns_len, &dbi);
    if (rc != 0) {
        if (rc == DML_DB_ERR_NOT_FOUND) {
            *out_exists = 0;
            rc = 0;
        }
        return rc;
    }
    db_key.mv_size = key_len;
    db_key.mv_data = (void *)key;
    rc = mdb_get((*p_txn)->txn, dbi, &db_key, &db_value);
    if (rc == MDB_NOTFOUND) {
        *out_exists = 0;
        return 0;
    }
    if (rc != MDB_SUCCESS) {
        return dml_map_lmdb_rc(rc);
    }
    *out_exists = 1;
    return 0;
}

int dml_db_iter_keys(
    DmlDbTxnHandle **p_txn,
    const char *ns,
    const char *start_token,
    DmlObjCollection *out_page
) {
    int rc = 0;
    MDB_dbi dbi;
    MDB_cursor *cursor = NULL;
    MDB_val db_key;
    MDB_val db_value;
    size_t count = 0;
    size_t keys_len = 0;
    size_t keys_cap = 0;
    char *keys = NULL;
    size_t *key_lens = NULL;
    DmlValue **values = NULL;
    char *next_token = NULL;

    if (out_page == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    out_page->keys = NULL;
    out_page->values = NULL;
    out_page->count = 0;
    out_page->next_token = NULL;

    if (ns == NULL || ns[0] == '\0') return DML_DB_ERR_INPUT_INVALID;
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;
    rc = dml_ns_dbi_lookup(p_txn, ns, strlen(ns), &dbi);
    if (rc != 0) {
        goto cleanup;
    }
    rc = mdb_cursor_open((*p_txn)->txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        rc = dml_map_lmdb_rc(rc);
        goto cleanup;
    }
    if (start_token != NULL && start_token[0] != '\0') {
        db_key.mv_data = (void *)start_token;
        db_key.mv_size = strlen(start_token);
        rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_SET_RANGE);
    } else {
        rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_FIRST);
    }
    if (rc != MDB_SUCCESS) {
        rc = (rc == MDB_NOTFOUND) ? 0 : dml_map_lmdb_rc(rc);
        goto cleanup;
    }
    values = (DmlValue **)calloc(DML_DB_ITER_LIMIT, sizeof(*values));
    key_lens = (size_t *)calloc(DML_DB_ITER_LIMIT, sizeof(*key_lens));
    if (values == NULL || key_lens == NULL) {
        rc = DML_DB_ERR_NOMEM;
        goto cleanup;
    }
    while (rc == MDB_SUCCESS && count < DML_DB_ITER_LIMIT) {
        size_t key_len = db_key.mv_size;
        size_t needed = keys_len + key_len + 1;
        DmlValue *value = NULL;

        if (needed > keys_cap) {
            size_t next_cap = keys_cap == 0 ? 128 : keys_cap * 2;
            if (next_cap < needed) next_cap = needed;
            char *next_keys = (char *)realloc(keys, next_cap);
            if (next_keys == NULL) {
                rc = DML_DB_ERR_NOMEM;
                goto cleanup;
            }
            keys = next_keys;
            keys_cap = next_cap;
        }
        if (dml_msgpack_unpack(db_value.mv_data, db_value.mv_size, &value) != 0 || value == NULL) {
            rc = DML_DB_ERR_MSGPACK;
            goto cleanup;
        }
        values[count] = value;
        key_lens[count] = key_len;
        memcpy(keys + keys_len, db_key.mv_data, key_len);
        keys_len += key_len;
        keys[keys_len] = '\0';
        keys_len += 1;
        count += 1;
        if (count >= DML_DB_ITER_LIMIT) {
            rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_NEXT);
            if (rc == MDB_SUCCESS) {
                size_t token_len = db_key.mv_size;
                next_token = (char *)malloc(token_len + 1);
                if (next_token == NULL) {
                    rc = DML_DB_ERR_NOMEM;
                    goto cleanup;
                }
                memcpy(next_token, db_key.mv_data, token_len);
                next_token[token_len] = '\0';
            } else if (rc == MDB_NOTFOUND) {
                rc = 0;
            } else {
                rc = dml_map_lmdb_rc(rc);
            }
            break;
        }
        rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_NEXT);
        if (rc == MDB_NOTFOUND) {
            rc = 0;
            break;
        }
        if (rc != MDB_SUCCESS) {
            rc = dml_map_lmdb_rc(rc);
            break;
        }
    }
    if (rc != 0) {
        goto cleanup;
    }
    out_page->keys = keys;
    out_page->key_lens = key_lens;
    out_page->values = values;
    out_page->count = count;
    out_page->next_token = next_token;
    keys = NULL;
    key_lens = NULL;
    values = NULL;
    next_token = NULL;

cleanup:
    if (cursor != NULL) mdb_cursor_close(cursor);
    if (values != NULL) {
        for (size_t i = 0; i < count; i++) {
            if (values[i] != NULL) {
                dml_value_free(values[i]);
            }
        }
        free(values);
    }
    free(keys);
    free(key_lens);
    free(next_token);
    return rc;
}

int dml_db_list_orphans(
    DmlDbTxnHandle **p_txn,
    const char *const *start_refs,
    size_t start_refs_count,
    DmlValue **out_refs
) {
    int rc = 0;
    DmlDumpList reachable = {0};
    DmlDumpList orphans = {0};
    MDB_dbi dbi;
    MDB_cursor *cursor = NULL;
    MDB_val db_key;
    MDB_val db_value;

    if (out_refs == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    *out_refs = NULL;
    if (start_refs_count > 0 && start_refs == NULL) {
        return DML_DB_ERR_INPUT_INVALID;
    }
    rc = dml_db_validate_txn(p_txn, 0);
    if (rc != 0) return rc;

    for (size_t i = 0; i < start_refs_count; i++) {
        if (start_refs[i] == NULL) {
            rc = DML_DB_ERR_INPUT_INVALID;
            goto cleanup;
        }
        size_t ref_len = strlen(start_refs[i]);
        rc = dml_dump_add_ref(p_txn, &reachable, start_refs[i], ref_len);
        if (rc != 0) {
            goto cleanup;
        }
    }

    for (size_t i = 0; i < (*p_txn)->db->namespace_count; i++) {
        const char *ns = (*p_txn)->db->namespaces[i];
        size_t ns_len = strlen(ns);

        rc = dml_ns_dbi_lookup(p_txn, ns, ns_len, &dbi);
        if (rc == DML_DB_ERR_NOT_FOUND) {
            rc = 0;
            continue;
        }
        if (rc != 0) {
            goto cleanup;
        }
        rc = mdb_cursor_open((*p_txn)->txn, dbi, &cursor);
        if (rc != MDB_SUCCESS) {
            rc = dml_map_lmdb_rc(rc);
            goto cleanup;
        }
        rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_FIRST);
        if (rc == MDB_NOTFOUND) {
            mdb_cursor_close(cursor);
            cursor = NULL;
            rc = 0;
            continue;
        }
        if (rc != MDB_SUCCESS) {
            rc = dml_map_lmdb_rc(rc);
            goto cleanup;
        }
        while (rc == MDB_SUCCESS) {
            size_t ref_len = ns_len + 1 + db_key.mv_size;
            char *ref_data = (char *)malloc(ref_len);
            if (ref_data == NULL) {
                rc = DML_DB_ERR_NOMEM;
                goto cleanup;
            }
            memcpy(ref_data, ns, ns_len);
        ref_data[ns_len] = ':';
            memcpy(ref_data + ns_len + 1, db_key.mv_data, db_key.mv_size);
            if (dml_dump_list_find(&reachable, ref_data, ref_len) < 0) {
                rc = dml_dump_list_add(&orphans, ref_data, ref_len, NULL);
                free(ref_data);
                if (rc != 0) goto cleanup;
            } else {
                free(ref_data);
            }
            rc = mdb_cursor_get(cursor, &db_key, &db_value, MDB_NEXT);
        }
        if (rc == MDB_NOTFOUND) {
            rc = 0;
        } else if (rc != 0) {
            rc = dml_map_lmdb_rc(rc);
        }
        mdb_cursor_close(cursor);
        cursor = NULL;
        if (rc != 0) goto cleanup;
    }

    DmlValue *result = dml_value_new_list(orphans.count);
    if (result == NULL) {
        rc = DML_DB_ERR_NOMEM;
        goto cleanup;
    }
    for (size_t i = 0; i < orphans.count; i++) {
        DmlDumpEntry *entry = &orphans.entries[i];
        DmlValue *ref_val = dml_value_new_ref(entry->key, entry->key_len);
        if (ref_val == NULL || dml_value_list_set(result, i, ref_val) != 0) {
            dml_value_free(ref_val);
            dml_value_free(result);
            rc = DML_DB_ERR_NOMEM;
            goto cleanup;
        }
    }
    *out_refs = result;
    result = NULL;

cleanup:
    if (cursor != NULL) mdb_cursor_close(cursor);
    dml_dump_list_free(&reachable);
    dml_dump_list_free(&orphans);
    return rc;
}

void dml_db_free_obj_collection(DmlObjCollection *page) {
    if (page == NULL) return;
    if (page->values != NULL) {
        for (size_t i = 0; i < page->count; i++) {
            if (page->values[i] != NULL) {
                dml_value_free(page->values[i]);
            }
        }
        free(page->values);
        page->values = NULL;
    }
    free(page->keys);
    page->keys = NULL;
    free(page->key_lens);
    page->key_lens = NULL;
    free(page->next_token);
    page->next_token = NULL;
    page->count = 0;
}
