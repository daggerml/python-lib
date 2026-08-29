#ifndef DAGGERML_DML_HASH_H
#define DAGGERML_DML_HASH_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

int dml_hash_sha256_hex(const void *data, size_t len, char out[65]);

#ifdef __cplusplus
}
#endif

#endif
