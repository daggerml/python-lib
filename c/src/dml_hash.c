#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../third_party/sha256/sha256.h"
#include "../include/dml_hash.h"

int
dml_hash_sha256_hex(const void *data, size_t len, char out[65])
{
    SHA256_CTX ctx;
    uint8_t hash[SHA256_BLOCK_SIZE];
    size_t i;

    if (out == NULL) {
        return -1;
    }

    sha256_init(&ctx);
    sha256_update(&ctx, (const uint8_t *)data, len);
    sha256_final(&ctx, hash);

    for (i = 0; i < SHA256_BLOCK_SIZE; i++) {
        snprintf(&out[i * 2], 3, "%02x", hash[i]);
    }
    out[64] = '\0';
    return 0;
}
