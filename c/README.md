# C Core

This document describes the current C implementation under `/c`.

## File layout

- `c/include/dml_db.h`: LMDB-backed database API.
- `c/include/dml_hash.h`: SHA-256 helper API.
- `c/include/dml_msgpack.h`: MessagePack encode/decode API.
- `c/include/dml_value.h`: `DmlValue` data model and helpers.
- `c/src/dml_db.c`: DB implementation.
- `c/src/dml_hash.c`: SHA-256 implementation wrapper.
- `c/src/dml_msgpack.c`: MessagePack implementation.
- `c/src/dml_value.c`: `DmlValue` implementation.
- `c/third_party`: vendored LMDB, msgpack-c, and SHA-256 sources.

## Notes

- Ref strings use `namespace:id` format (colon separator).
- This tree does not currently include `c/bindings/cpython/py_module.c`.
- The primary DB source file is `c/src/dml_db.c` (not `dml_core.c`).

## `c/include/dml_db.h`

### Types and constants

- `DML_DB_ITER_LIMIT`
- `DmlDbHandle`
- `DmlObjCollection`
- DB error codes `DML_DB_OK` through `DML_DB_ERR_MAP_SIZE_MAX`

### Public API

- `int dml_db_txn_open(const char *path, const char *const *namespaces, size_t namespace_count, const int readonly, const int create_if_missing, size_t map_size, DmlDbHandle **out_txn)` (`map_size` applies only when opening an unleased environment)
- `int dml_db_txn_close(DmlDbHandle **p_txn, const int commit)`
- `int dml_db_resize(const char *path, const char *const *namespaces, size_t namespace_count, const int create_if_missing, size_t headroom, size_t max_map_size, size_t *out_current_map_size)` (reads LMDB's current map size, grows it by up to `headroom`, and returns `DML_DB_ERR_MAP_SIZE_MAX` when it cannot grow)
- `int dml_db_put(DmlDbHandle **p_txn, const char *ns, size_t ns_len, const char *key, size_t key_len, const DmlValue *value, int no_overwrite, int raw, DmlValue **out_ref)`
- `int dml_db_get(DmlDbHandle **p_txn, const char *ns, size_t ns_len, const char *key, size_t key_len, int raw, DmlValue **out_value)`
- `int dml_db_del(DmlDbHandle **p_txn, const char *ns, size_t ns_len, const char *key, size_t key_len)`
- `int dml_db_exists(DmlDbHandle **p_txn, const char *ns, size_t ns_len, const char *key, size_t key_len, int *out_exists)`
- `int dml_db_iter_keys(struct DmlDbHandle **p_txn, const char *ns, const char *start_token, DmlObjCollection *out_page)`
- `void dml_db_free_obj_collection(DmlObjCollection *page)`
- `int dml_db_list_orphans(struct DmlDbHandle **p_txn, const char *const *start_refs, size_t start_refs_count, const char *const *missing_commit_refs, size_t missing_commit_refs_count, DmlValue **out_refs)`

## `c/src/dml_db.c`

### Internal structs

- `struct DmlDbHandle`
- `DmlDumpEntry`
- `DmlDumpList`

### Internal helpers

- `static int dml_map_lmdb_rc(int rc)`
- `static void dml_dump_list_free(DmlDumpList *list)`
- `static int dml_dump_list_find(const DmlDumpList *list, const char *key, size_t key_len)`
- `static int dml_dump_list_add(DmlDumpList *list, const char *key, size_t key_len, DmlValue *value)`
- `static int dml_db_open_env_locked(DmlDbRegistryEntry *entry, int create_if_missing, size_t map_size)`
- `static int dml_db_reopen_slot_locked(size_t slot, int create_if_missing)`
- `static int dml_db_grow_slot_locked(size_t slot, int create_if_missing, size_t headroom, size_t max_map_size, size_t *out_current_map_size)`
- `static int dml_db_validate_txn(struct DmlDbHandle **p_handle)`

### Exported functions

- `int dml_db_txn_open(...)`
- `int dml_db_resize(...)`
- `int dml_db_txn_close(...)`
- `int dml_db_put(...)`
- `int dml_db_get(...)`
- `int dml_db_del(...)`
- `int dml_db_exists(...)`
- `int dml_db_iter_keys(...)`
- `int dml_db_list_orphans(...)`
- `void dml_db_free_obj_collection(...)`

## `c/include/dml_hash.h` and `c/src/dml_hash.c`

- `int dml_hash_sha256_hex(const void *data, size_t len, char out[65])`

## `c/include/dml_msgpack.h` and `c/src/dml_msgpack.c`

### Types and constants

- `DmlMsgpackBuffer`
- `DML_MSGPACK_OK`
- `DML_MSGPACK_ERR_INVALID`
- `DML_MSGPACK_ERR_NOMEM`
- `DML_MSGPACK_EXT_REF`

### API

- `int dml_msgpack_pack(const DmlValue *value, DmlMsgpackBuffer *out_buffer)`
- `int dml_msgpack_unpack(const char *data, size_t size, DmlValue **out_value)`
- `void dml_msgpack_free_buffer(void *data)`

### Internal helpers

- `static int dml_msgpack_entry_compare(const void *left, const void *right)`
- `static int dml_msgpack_pack_value(msgpack_packer *packer, const DmlValue *value)`
- `static DmlValue *dml_msgpack_from_object(const msgpack_object *obj)`

## `c/include/dml_value.h` and `c/src/dml_value.c`

### Types and constants

- `DmlValueType` (`DML_VALUE_NULL`, `DML_VALUE_BOOL`, `DML_VALUE_INT`, `DML_VALUE_FLOAT`, `DML_VALUE_STR`, `DML_VALUE_LIST`, `DML_VALUE_MAP`, `DML_VALUE_REF`)
- `DmlValue`
- `DmlMapEntry`
- `DML_REF_ID_MAX`

### API

- `DmlValue *dml_value_new_null(void)`
- `DmlValue *dml_value_new_bool(int value)`
- `DmlValue *dml_value_new_int(long long value)`
- `DmlValue *dml_value_new_float(double value)`
- `DmlValue *dml_value_new_str(const char *data, size_t size)`
- `DmlValue *dml_value_new_ref(const char *data, size_t size)`
- `DmlValue *dml_value_new_list(size_t count)`
- `int dml_value_list_set(DmlValue *list, size_t index, DmlValue *item)`
- `DmlValue *dml_value_new_map(size_t count)`
- `int dml_value_map_set(DmlValue *map, size_t index, const char *key, size_t key_len, DmlValue *value)`
- `int dml_value_map_sort(DmlValue *map)`
- `void dml_value_free(DmlValue *value)`
- `int dml_ref_split(const char *ref, size_t ref_len, const char **namespace_str, size_t *namespace_len, const char **id_str, size_t *id_len)`

### Internal helpers

- `static int dml_value_map_entry_compare(const void *left, const void *right)`
- `static DmlValue *dml_value_alloc(DmlValueType type)`
