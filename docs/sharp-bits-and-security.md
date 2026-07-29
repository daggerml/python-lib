# Sharp bits and security

## Sharp bits

### A repository without `remote.root` is read-only

A `Dml` instance without a configured `remote.root` supports read-only inspection. Runtime authoring and mutation methods require `remote.root`; configure one before creating, staging, committing, or executing a DAG.

### Editable dependency changes do not affect a funk cache key

The cache key for a script funk includes the rendered function source and normalized DaggerML inputs. It does not include the implementation of modules imported by that function. This matters when an imported module comes from an editable installation.

For example, suppose the editable `foo` package initially contains:

```python
# foo/bar.py
def normalize(value):
    return value.lower()
```

An author creates this funk:

```python
@api.funkify
def normalize_text(dag, value):
    from foo.bar import normalize

    return dag.put(normalize(value.value()))
```

After that funk has completed for a given input, changing `foo.bar.normalize()` to return `value.upper()` does not change the rendered `normalize_text` source or its cache key. Calling `normalize_text` with the same input would therefore reuse the completed DAG containing the lowercase result.
To include an inspectable helper's source in the rendered script and cache identity, pass it through `extra_objs`:

```python
from foo.bar import normalize

@api.funkify(extra_objs=(normalize,))
def normalize_text(dag, value):
    return dag.put(normalize(value.value()))
```

Do not expect edits to an editable dependency to invalidate a funk cache. Invalidate the cache or make the dependency change part of the staged runnable before recomputing.

### Error nodes cannot be traversed in Python

A failed function call remains a named node in its parent DAG, but loading that node in Python immediately re-raises its stored error. For example, `dag["err-val"]` raises, so `dag["err-val"].context().intermediate_node` cannot be used to inspect values in the failed function DAG.

Use the CLI to resolve the failed node and find its function DAG reference. First use `dml show` to find the parent DAG reference, then inspect the named node:

```bash
dml dag get-node-by-name PARENT_DAG_REF err-val
dml dag describe-node ERROR_NODE_REF
```

`describe-node` reports the function DAG reference. Construct that DAG explicitly in Python to inspect its named nodes:

```python
error_dag = dml.Dag(dml=dag.dml, ref=error_dag_ref)
print(error_dag.intermediate_node.value())
```

This limitation will be addressed in a future release.

## Known security holes

### Runnable adapters can launch arbitrary local executables

The `adapter` field is persisted in each runnable. When the runnable executes, the runtime passes that field to `shutil.which()` and then launches the returned path with `subprocess.run()`. It does not check that the field names a registered adapter executable. An absolute path is accepted as well as a name resolved through `PATH`.

For example, on a shared execution host, suppose an attacker can place this executable at `/tmp/dml-adapter`:

```sh
#!/bin/sh
touch /tmp/dml-adapter-ran
printf '%s\n' '{"status":"failed","error":"attacker adapter ran"}'
```

They can provide a runnable whose adapter is `/tmp/dml-adapter`. When that runnable is executed, DaggerML launches `/tmp/dml-adapter`; the marker file demonstrates that the executable ran. The adapter request is JSON on standard input, so this is arbitrary executable selection, not shell interpolation of request content.

### Tar archives can write outside their extraction destination

`S3Store.untar()` rejects member names that are absolute or contain path traversal, but it accepts symlink and hardlink members. An archive can first create a symlink named `link` to `/tmp/outside`, then contain a regular file named `link/payload`. Both member names pass validation because they are under the destination. During extraction, the second member is written through the symlink to `/tmp/outside/payload`.

Do not create tarballs with symlink or hardlink members for use with `S3Store.untar()`.

### Hash-derived object names are not verified when read

DaggerML uses hash-derived names for stored artifacts and other managed objects, but it does not recompute and compare those hashes when it reads them. Overwriting a known object or ref can therefore replace the data DaggerML uses without a hash mismatch being detected. This includes script source: overwritten script bytes can later be executed by a script worker.

Never manually overwrite DaggerML-managed objects or refs. Humans and automated agents must use DaggerML tooling to write and update them.

### SSH flags can run commands on the local execution host

The SSH executor passes every supplied `flags` entry directly to the local `ssh` command. OpenSSH options such as `ProxyCommand` run a local command before the SSH connection is made.

For example, an SSH runnable with `flags=["-oProxyCommand=touch /tmp/dml-proxy-command"]` causes the local SSH client to run `touch /tmp/dml-proxy-command`. The command is run on the host executing DaggerML, not the remote SSH host.

Use only SSH flags you control. Do not pass untrusted values as SSH flags.
