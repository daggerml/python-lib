# LMDB

We vendored LMDB into `c/third_party/lmdb` from the canonical upstream release:

- Source: https://github.com/LMDB/lmdb
- Release tag: `LMDB_0.9.31`

Steps used:

1. Download the release tarball for `LMDB_0.9.31`.
2. Extract only `libraries/liblmdb`.
3. Prune the directory to the minimal build inputs and licenses:
   - `mdb.c`, `midl.c`
   - `lmdb.h`, `midl.h`
   - `LICENSE`, `COPYRIGHT`
4. Remove the tarball after extraction.

If we update LMDB, repeat the same steps and keep the version pinned in this section.
