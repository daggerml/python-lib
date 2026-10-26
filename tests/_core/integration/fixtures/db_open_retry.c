/* Test-only interception: production code and vendored LMDB have no hooks. */
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include "lmdb.h"

static int open_calls, create_calls, missing_opens;
static int injected_error, failures_remaining;
static int force_race;
static int ready_pipe[2], resume_pipe[2];

static int test_env_create(MDB_env **env) {
    create_calls++;
    return mdb_env_create(env);
}

static int test_env_open(MDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode) {
    int rc;
    open_calls++;
    if (failures_remaining > 0) {
        failures_remaining--;
        return injected_error;
    }
    rc = mdb_env_open(env, path, flags, mode);
    if (rc == ENOENT) missing_opens++;
    return rc;
}

#define mdb_env_create test_env_create
#define mdb_env_open test_env_open
#include "src/dml_db.c"
#undef mdb_env_create
#undef mdb_env_open

/* Only LMDB's translation unit is compiled with this fcntl replacement. */
int test_fcntl(int fd, int command, ...) {
    va_list args;
    int result;
    va_start(args, command);
    if (command == F_SETLK || command == F_SETLKW || command == F_GETLK) {
        struct flock *lock = va_arg(args, struct flock *);
        if (force_race && command == F_SETLKW && lock->l_type == F_RDLCK) {
            char byte;
            force_race = 0;
            /* The initial exclusive lock already failed. Allow the parent
             * to close and unlink semaphores before taking the shared lock. */
            assert(write(ready_pipe[1], "r", 1) == 1);
            assert(read(resume_pipe[0], &byte, 1) == 1);
        }
        result = fcntl(fd, command, lock);
    } else if (command == F_GETFD || command == F_GETFL) {
        result = fcntl(fd, command);
    } else {
        int argument = va_arg(args, int);
        result = fcntl(fd, command, argument);
    }
    va_end(args);
    return result;
}

static DmlDbRegistryEntry entry_for(const char *path) {
    static char *namespaces[] = {"datum"};
    DmlDbRegistryEntry entry = {0};
    entry.path = (char *)path;
    entry.namespaces = namespaces;
    entry.namespace_count = 1;
    return entry;
}

static void write_value(DmlDbRegistryEntry *entry) {
    MDB_txn *txn;
    MDB_val key = {3, "key"}, value = {5, "child"};
    assert(mdb_txn_begin(entry->env, NULL, 0, &txn) == 0);
    assert(mdb_put(txn, entry->dbis[0], &key, &value, 0) == 0);
    assert(mdb_txn_commit(txn) == 0);
}

int main(int argc, char **argv) {
    MDB_env *parent;
    int rc;
    char missing_path[4096];
    assert(argc == 3);
    const char *mode = argv[1], *path = argv[2];
    assert(mdb_env_create(&parent) == 0);
    assert(mdb_env_open(parent, path, 0, 0600) == 0);

    if (strcmp(mode, "race") == 0) {
        assert(pipe(ready_pipe) == 0 && pipe(resume_pipe) == 0);
        pid_t child = fork();
        assert(child >= 0);
        if (child == 0) {
            force_race = 1;
            close(ready_pipe[0]);
            close(resume_pipe[1]);
            DmlDbRegistryEntry entry = entry_for(path);
            rc = dml_db_open_env_locked(&entry, 0, 1024 * 1024);
            if (rc != 0) {
                fprintf(stderr, "child open failed: %d\n", rc);
                _exit(1);
            }
            assert(missing_opens == 1 && open_calls == 2 && create_calls == 2);
            write_value(&entry);
            dml_db_close_env(&entry);
            _exit(0);
        }
        close(ready_pipe[1]);
        close(resume_pipe[0]);
        char byte;
        assert(read(ready_pipe[0], &byte, 1) == 1);
        mdb_env_close(parent);
        assert(write(resume_pipe[1], "g", 1) == 1);
        int status;
        assert(waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        DmlDbRegistryEntry entry = entry_for(path);
        assert(dml_db_open_env_locked(&entry, 0, 1024 * 1024) == 0);
        MDB_txn *txn;
        MDB_val key = {3, "key"}, value;
        assert(mdb_txn_begin(entry.env, NULL, MDB_RDONLY, &txn) == 0);
        assert(mdb_get(txn, entry.dbis[0], &key, &value) == 0);
        assert(value.mv_size == 5 && memcmp(value.mv_data, "child", 5) == 0);
        mdb_txn_abort(txn);
        dml_db_close_env(&entry);
        puts("forced semaphore race recovered and child write remains readable");
        return 0;
    }

    mdb_env_close(parent);
    injected_error = strcmp(mode, "permission") == 0 ? EACCES : ENOENT;
    failures_remaining = strcmp(mode, "transient") == 0 ? 1 : 100;
    if (strcmp(mode, "missing-data") == 0) {
        char file[4096];
        assert(snprintf(file, sizeof(file), "%s/data.mdb", path) < (int)sizeof(file));
        assert(unlink(file) == 0);
    } else if (strcmp(mode, "missing-dir") == 0) {
        assert(snprintf(missing_path, sizeof(missing_path), "%s/missing", path) < (int)sizeof(missing_path));
        path = missing_path;
    }
    DmlDbRegistryEntry entry = entry_for(path);
    rc = dml_db_open_env_locked(&entry, 0, 1024 * 1024);
    if (strcmp(mode, "transient") == 0) {
        assert(rc == 0 && open_calls == 2 && create_calls == 2);
        assert(entry.env != NULL && entry.dbis != NULL && entry.env_refcount == 1);
        write_value(&entry);
        dml_db_close_env(&entry);
    } else {
        int expected = strcmp(mode, "persistent") == 0 ? DML_DB_ENV_OPEN_ATTEMPTS : 1;
        assert(rc == DML_DB_ERR_PATH_INVALID);
        assert(open_calls == expected && create_calls == expected);
        assert(entry.env == NULL && entry.dbis == NULL && entry.env_refcount == 0);
    }
    return 0;
}
