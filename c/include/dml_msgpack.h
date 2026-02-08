#ifndef DAGGERML_DML_MSGPACK_H
#define DAGGERML_DML_MSGPACK_H

#include <stddef.h>

#include "dml_value.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    void *data;
    size_t size;
} DmlMsgpackBuffer;

enum {
    DML_MSGPACK_OK = 0,
    DML_MSGPACK_ERR_INVALID = -1,
    DML_MSGPACK_ERR_NOMEM = -2
};

enum {
    DML_MSGPACK_EXT_REF = 1
};

int dml_msgpack_pack(const DmlValue *value, DmlMsgpackBuffer *out_buffer);
int dml_msgpack_unpack(const char *data, size_t size, DmlValue **out_value);
void dml_msgpack_free_buffer(void *data);

#ifdef __cplusplus
}
#endif

#endif
