#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "include/dml_db.h"

static int remove_tree(const char *path) {
    DIR *dir = NULL;
    struct dirent *entry = NULL;

    dir = opendir(path);
    if (dir == NULL) {
        return -1;
    }
    while ((entry = readdir(dir)) != NULL) {
        char child[4096];
        struct stat st;

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) >= (int)sizeof(child)) {
            closedir(dir);
            errno = ENAMETOOLONG;
            return -1;
        }
        if (stat(child, &st) != 0) {
            closedir(dir);
            return -1;
        }
        if (S_ISDIR(st.st_mode)) {
            if (remove_tree(child) != 0) {
                closedir(dir);
                return -1;
            }
        } else if (unlink(child) != 0) {
            closedir(dir);
            return -1;
        }
    }
    closedir(dir);
    return rmdir(path);
}

static int expect_string_value(const DmlValue *value, const char *expected, size_t expected_len) {
    if (value == NULL || value->type != DML_VALUE_STR) {
        return 0;
    }
    if (value->as.str.size != expected_len) {
        return 0;
    }
    return memcmp(value->as.str.data, expected, expected_len) == 0;
}

int main(void) {
    const char *namespaces[] = {"datum"};
    const char payload[] = "hello from smoke test";
    char tmpdir[] = "/tmp/opencode/dml-db-smoke-XXXXXX";
    DmlDbHandle *db = NULL;
    DmlValue *input = NULL;
    DmlValue *ref = NULL;
    DmlValue *output = NULL;
    const char *ref_ns = NULL;
    const char *ref_id = NULL;
    size_t ref_ns_len = 0;
    size_t ref_id_len = 0;
    size_t db_size = 0;
    int rc = 0;
    int exit_code = 1;

    if (mkdtemp(tmpdir) == NULL) {
        perror("mkdtemp");
        return 1;
    }

    rc = dml_db_get_size(tmpdir, &db_size);
    if (rc != 0) {
        fprintf(stderr, "dml_db_get_size(empty) failed: %d\n", rc);
        goto cleanup;
    }
    if (db_size != 0) {
        fprintf(stderr, "expected empty db size, got: %zu\n", db_size);
        goto cleanup;
    }

    rc = dml_db_open(tmpdir, namespaces, 1, 1, 0, 0, &db);
    if (rc != 0) {
        fprintf(stderr, "dml_db_open(write) failed: %d\n", rc);
        goto cleanup;
    }

    input = dml_value_new_str(payload, sizeof(payload) - 1);
    if (input == NULL) {
        fprintf(stderr, "dml_value_new_str failed\n");
        goto cleanup;
    }

    rc = dml_db_put(&db, "datum", strlen("datum"), NULL, 0, input, 0, 0, &ref);
    if (rc != 0) {
        fprintf(stderr, "dml_db_put failed: %d\n", rc);
        goto cleanup;
    }
    if (ref == NULL || ref->type != DML_VALUE_REF) {
        fprintf(stderr, "dml_db_put did not return a ref\n");
        goto cleanup;
    }

    rc = dml_ref_split(ref->as.ref.data, ref->as.ref.size, &ref_ns, &ref_ns_len, &ref_id, &ref_id_len);
    if (rc != 0) {
        fprintf(stderr, "dml_ref_split failed: %d\n", rc);
        goto cleanup;
    }

    rc = dml_db_get(&db, ref_ns, ref_ns_len, ref_id, ref_id_len, 0, &output);
    if (rc != 0) {
        fprintf(stderr, "dml_db_get(write txn) failed: %d\n", rc);
        goto cleanup;
    }
    if (!expect_string_value(output, payload, sizeof(payload) - 1)) {
        fprintf(stderr, "unexpected value from write txn\n");
        goto cleanup;
    }
    dml_value_free(output);
    output = NULL;

    rc = dml_db_close(&db, 1);
    if (rc != 0) {
        fprintf(stderr, "dml_db_close(commit) failed: %d\n", rc);
        goto cleanup;
    }

    rc = dml_db_get_size(tmpdir, &db_size);
    if (rc != 0) {
        fprintf(stderr, "dml_db_get_size(populated) failed: %d\n", rc);
        goto cleanup;
    }
    if (db_size == 0) {
        fprintf(stderr, "expected populated db size to be non-zero\n");
        goto cleanup;
    }

    rc = dml_db_open(tmpdir, namespaces, 1, 0, 0, 1, &db);
    if (rc != 0) {
        fprintf(stderr, "dml_db_open(read) failed: %d\n", rc);
        goto cleanup;
    }

    rc = dml_db_get(&db, ref_ns, ref_ns_len, ref_id, ref_id_len, 0, &output);
    if (rc != 0) {
        fprintf(stderr, "dml_db_get(read txn) failed: %d\n", rc);
        goto cleanup;
    }
    if (!expect_string_value(output, payload, sizeof(payload) - 1)) {
        fprintf(stderr, "unexpected value from read txn\n");
        goto cleanup;
    }

    printf("retrieved: %.*s\n", (int)output->as.str.size, output->as.str.data);
    exit_code = 0;

cleanup:
    if (output != NULL) {
        dml_value_free(output);
    }
    if (ref != NULL) {
        dml_value_free(ref);
    }
    if (input != NULL) {
        dml_value_free(input);
    }
    if (db != NULL) {
        int close_rc = dml_db_close(&db, 0);

        if (close_rc != 0 && exit_code == 0) {
            fprintf(stderr, "dml_db_close(cleanup) failed: %d\n", close_rc);
            exit_code = 1;
        }
    }
    if (remove_tree(tmpdir) != 0) {
        perror("remove_tree");
        return 1;
    }
    return exit_code;
}
