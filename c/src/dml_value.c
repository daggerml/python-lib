#include <stdlib.h>
#include <string.h>

#include "../include/dml_value.h"

static int
dml_value_map_entry_compare(const void *left, const void *right)
{
    const DmlMapEntry *a = (const DmlMapEntry *)left;
    const DmlMapEntry *b = (const DmlMapEntry *)right;
    size_t min_len = a->key_len < b->key_len ? a->key_len : b->key_len;
    int cmp = 0;

    if (min_len > 0) {
        cmp = memcmp(a->key, b->key, min_len);
    }
    if (cmp != 0) {
        return cmp;
    }
    if (a->key_len < b->key_len) {
        return -1;
    }
    if (a->key_len > b->key_len) {
        return 1;
    }
    return 0;
}

static DmlValue *
dml_value_alloc(DmlValueType type)
{
    DmlValue *value = (DmlValue *)calloc(1, sizeof(*value));
    if (value == NULL) {
        return NULL;
    }
    value->type = type;
    return value;
}

DmlValue *
dml_value_new_null(void)
{
    return dml_value_alloc(DML_VALUE_NULL);
}

DmlValue *
dml_value_new_bool(int value)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_BOOL);
    if (result == NULL) {
        return NULL;
    }
    result->as.boolean = value ? 1 : 0;
    return result;
}

DmlValue *
dml_value_new_int(long long value)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_INT);
    if (result == NULL) {
        return NULL;
    }
    result->as.integer = value;
    return result;
}

DmlValue *
dml_value_new_float(double value)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_FLOAT);
    if (result == NULL) {
        return NULL;
    }
    result->as.floating = value;
    return result;
}

DmlValue *
dml_value_new_str(const char *data, size_t size)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_STR);
    if (result == NULL) {
        return NULL;
    }
    result->as.str.data = (char *)malloc(size);
    if (result->as.str.data == NULL) {
        free(result);
        return NULL;
    }
    if (size > 0 && data != NULL) {
        memcpy(result->as.str.data, data, size);
    }
    result->as.str.size = size;
    return result;
}

DmlValue *
dml_value_new_ref(const char *data, size_t size)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_REF);
    if (result == NULL) {
        return NULL;
    }
    result->as.ref.data = (char *)malloc(size);
    if (result->as.ref.data == NULL) {
        free(result);
        return NULL;
    }
    if (size > 0 && data != NULL) {
        memcpy(result->as.ref.data, data, size);
    }
    result->as.ref.size = size;
    return result;
}

DmlValue *
dml_value_new_list(size_t count)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_LIST);
    if (result == NULL) {
        return NULL;
    }
    if (count == 0) {
        return result;
    }
    result->as.list.items = (DmlValue **)calloc(count, sizeof(DmlValue *));
    if (result->as.list.items == NULL) {
        free(result);
        return NULL;
    }
    result->as.list.count = count;
    return result;
}

int
dml_value_list_set(DmlValue *list, size_t index, DmlValue *item)
{
    if (list == NULL || list->type != DML_VALUE_LIST) {
        return -1;
    }
    if (index >= list->as.list.count) {
        return -1;
    }
    list->as.list.items[index] = item;
    return 0;
}

DmlValue *
dml_value_new_map(size_t count)
{
    DmlValue *result = dml_value_alloc(DML_VALUE_MAP);
    if (result == NULL) {
        return NULL;
    }
    if (count == 0) {
        return result;
    }
    result->as.map.entries = (DmlMapEntry *)calloc(count, sizeof(DmlMapEntry));
    if (result->as.map.entries == NULL) {
        free(result);
        return NULL;
    }
    result->as.map.count = count;
    return result;
}

int
dml_value_map_set(DmlValue *map, size_t index, const char *key, size_t key_len, DmlValue *value)
{
    char *key_copy = NULL;

    if (map == NULL || map->type != DML_VALUE_MAP) {
        return -1;
    }
    if (index >= map->as.map.count) {
        return -1;
    }

    key_copy = (char *)malloc(key_len);
    if (key_copy == NULL) {
        return -1;
    }
    if (key_len > 0 && key != NULL) {
        memcpy(key_copy, key, key_len);
    }

    map->as.map.entries[index].key = key_copy;
    map->as.map.entries[index].key_len = key_len;
    map->as.map.entries[index].value = value;
    return 0;
}

int
dml_value_map_sort(DmlValue *map)
{
    if (map == NULL || map->type != DML_VALUE_MAP) {
        return -1;
    }
    if (map->as.map.count < 2) {
        return 0;
    }
    qsort(map->as.map.entries, map->as.map.count, sizeof(DmlMapEntry), dml_value_map_entry_compare);
    return 0;
}

void
dml_value_free(DmlValue *value)
{
    size_t i;

    if (value == NULL) {
        return;
    }

    switch (value->type) {
    case DML_VALUE_STR:
        free(value->as.str.data);
        break;
    case DML_VALUE_LIST:
        for (i = 0; i < value->as.list.count; i++) {
            dml_value_free(value->as.list.items[i]);
        }
        free(value->as.list.items);
        break;
    case DML_VALUE_MAP:
        for (i = 0; i < value->as.map.count; i++) {
            free(value->as.map.entries[i].key);
            dml_value_free(value->as.map.entries[i].value);
        }
        free(value->as.map.entries);
        break;
    case DML_VALUE_REF:
        free(value->as.ref.data);
        break;
    default:
        break;
    }

    free(value);
}

int
dml_ref_split(
    const char *ref,
    size_t ref_len,
    const char **namespace_str,
    size_t *namespace_len,
    const char **id_str,
    size_t *id_len
)
{
    const char *slash = NULL;
    size_t ns_len = 0;
    size_t id_size = 0;

    if (ref == NULL || ref_len == 0 || namespace_str == NULL || namespace_len == NULL || id_str == NULL ||
        id_len == NULL) {
        return -1;
    }

    slash = memchr(ref, ':', ref_len);
    if (slash == NULL || slash == ref) {
        return -1;
    }

    ns_len = (size_t)(slash - ref);
    id_size = ref_len - ns_len - 1;
    if (id_size > DML_REF_ID_MAX) {
        return -1;
    }

    *namespace_str = ref;
    *namespace_len = ns_len;
    *id_str = slash + 1;
    *id_len = id_size;
    return 0;
}
