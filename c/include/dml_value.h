#ifndef DAGGERML_DML_VALUE_H
#define DAGGERML_DML_VALUE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DML_VALUE_NULL = 0,
    DML_VALUE_BOOL,
    DML_VALUE_INT,
    DML_VALUE_FLOAT,
    DML_VALUE_STR,
    DML_VALUE_LIST,
    DML_VALUE_MAP,
    DML_VALUE_REF
} DmlValueType;

typedef struct DmlValue DmlValue;

enum {
    DML_REF_ID_MAX = 64
};

typedef struct {
    char *key;
    size_t key_len;
    DmlValue *value;
} DmlMapEntry;

struct DmlValue {
    DmlValueType type;
    union {
        int boolean;
        long long integer;
        double floating;
        struct {
            char *data;
            size_t size;
        } str;
        struct {
            DmlValue **items;
            size_t count;
        } list;
        struct {
            DmlMapEntry *entries;
            size_t count;
        } map;
        struct {
            char *data;
            size_t size;
        } ref;
    } as;
};

DmlValue *dml_value_new_null(void);
DmlValue *dml_value_new_bool(int value);
DmlValue *dml_value_new_int(long long value);
DmlValue *dml_value_new_float(double value);
DmlValue *dml_value_new_str(const char *data, size_t size);
DmlValue *dml_value_new_ref(const char *data, size_t size);
DmlValue *dml_value_new_list(size_t count);
int dml_value_list_set(DmlValue *list, size_t index, DmlValue *item);
DmlValue *dml_value_new_map(size_t count);
int dml_value_map_set(DmlValue *map, size_t index, const char *key, size_t key_len, DmlValue *value);
int dml_value_map_sort(DmlValue *map);
void dml_value_free(DmlValue *value);
int dml_ref_split(
    const char *ref,
    size_t ref_len,
    const char **namespace_str,
    size_t *namespace_len,
    const char **id_str,
    size_t *id_len
);

#ifdef __cplusplus
}
#endif

#endif
