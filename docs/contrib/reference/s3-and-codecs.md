# S3 And Codecs

## `S3Store`

`daggerml.contrib.s3.S3Store` is the contrib utility for storing external payloads in S3-backed object storage.

When you construct it without explicit `bucket` and `prefix`, it reads `remote.root` from the active DaggerML config and derives a data prefix at `<remote-root-path>/data`.

That keeps contrib artifact storage separate from the repository-managed `dml/` protocol namespace.

## Main `S3Store` operations

| Method | Purpose |
| --- | --- |
| `parse_uri(...)` | Normalize a name, `Uri`, or node-like value into `(bucket, key)`. |
| `put(...)` | Content-addressed write of bytes or a local file. |
| `get(...)` | Read raw bytes. |
| `exists(...)` | Existence check using `head_object`. |
| `ls(...)` | List object URIs from the current prefix or a supplied root. |
| `rm(...)` | Delete one or more objects. |
| `put_js(...)` / `get_js(...)` | JSON helpers. |
| `tar(...)` / `untar(...)` | Archive a local directory or unpack a stored tarball. |
| `cd(...)` | Rebase to a different prefix while preserving the client and bucket. |

Important behavior:

- writes are content-addressed by `sha256(payload_bytes) + suffix`,
- `tar(...)` normalizes archive metadata for deterministic output,
- `untar(..., unsafe=False)` rejects absolute paths and destination-escaping members,
- `is_s3_uri(value)` only returns `True` for non-empty `s3://bucket/key` values.

## Built-in contrib codecs

`daggerml.contrib.codecs.literal_codecs()` returns the built-in contrib dataframe codecs that are available in the current process.

Current catalog:

- pandas `DataFrame` codec
- polars `DataFrame` codec

Both codecs:

- match only their own dataframe type,
- serialize to parquet bytes,
- publish those bytes through `S3Store.put(..., suffix=".parquet")`,
- return an external `Uri` rather than in-repo literal storage.

If the optional backend library is not installed, that codec simply does not appear in `literal_codecs()`.

## Where these surfaces show up

- `docker_build` uses `S3Store` for build contexts and image tarballs.
- `script` uses `S3Store` for serialized script artifacts.
- `docker` may load an image tar from an S3 `Uri`.
- dataframe values can be externalized automatically through the contrib codecs.

See also: [runtime surfaces](runtime-surfaces.md)
