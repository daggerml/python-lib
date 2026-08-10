# Sharp bits and security

## Sharp bits

### A repository without `remote.root` is read-only

A `Dml` instance without a configured `remote.root` supports read-only inspection. Runtime authoring and mutation methods require `remote.root`; configure one before creating, staging, committing, or executing a DAG.

### Reconstructing a frozen DAG can lose commit metadata

Freezing preserves the runtime index and its partial graph, but not the in-memory metadata on its Python `Dag` wrapper. `dml.resume()` therefore requires explicit `name`, `message`, and `tags` arguments; it does not restore them from the frozen token. Supplying `tags=None` explicitly means that the later commit has no tags.

Prefer retaining the original `Dag` instance and calling `unfreeze()` on it. If reconstruction is necessary, call `dml.resume(frozen, name=..., message=..., tags=...)` before committing; do not assume metadata can be recovered from the frozen index or freeze message.

### Editable dependency changes do not affect a funk cache key

The cache key for a script funk includes the rendered function source and normalized DaggerML inputs. It does not include the implementation of modules imported by that function. This matters when an imported module comes from an editable or updated installation.

For example, suppose the editable `foo` package initially contains:

```python
# foo/bar.py
def mean(vals):
    return sum(vals) / len(vals)

def variance(vals):
    return sum([(x - mean(vals)) ** 2 for x in vals]) / len(vals)
```

An author creates this funk:

```python
@api.funkify
def compute_stats(dag, numbers):
    from foo.bar import variance

    return variance(numbers.value())
```

and executes
```python
dag.call(compute_stats, [1, 2, 3]).value() == 2/3
```

The author later realizes that this is the formula for the population variance and not the sample variance! He or she fixes the error and reruns the analysis:
```python
# foo/bar.py
def mean(vals):
    return sum(vals) / len(vals)

def variance(vals):
    return sum([(x - mean(vals)) ** 2 for x in vals]) / (len(vals) - 1)
```

An author creates this funk:

```python
@api.funkify
def compute_stats(dag, numbers):
    from foo.bar import variance

    return variance(numbers.value())
```

and executes

```python
dag.call(compute_stats, [1, 2, 3]).value() == 2/3
dag.call(compute_stats, [2, 1, 3]).value() == 2/2
```

The old result is still being used in the cache! This is because we never included the implementation of `variance` was never included in the cache key. You can invalidate the cache for that item, or you can include the implementation *in* the funk itself.
To include an inspectable helper's source in the rendered script and cache identity, pass it through `extra_objs`:

```python
from foo.bar import mean, variance

@api.funkify(extra_objs=(mean,variance))
def compute_stats(dag, value):
    return variance(value.value())
```

Note that we must include both `mean` and `variance`. One can include other things like `import numpy as np` via `extra_lines`:

```python
# foo/bar.py
import numpy as np

def variance(vals):
    return np.variance(vals)

@api.funkify(extra_objs=(variance,), extra_lines=["import numpy as np"])
def compute_stats(dag, value):
    return variance(value.value())
```

### Do not run administrative work while pulling

Do not run `dml admin` commands concurrently with `dml pull` or other remote synchronization against the same local repository. In particular, local garbage collection can remove an object while a pull is materializing a remote object graph, leaving the materialized graph with a missing dependency. Run administrative work only after synchronization has completed.

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

### Remote CAS dependencies are traversed before identity verification

When materializing a remote CAS object graph, DaggerML decodes each downloaded object and follows its references before verifying that the object's content produces the requested local object ref. The later local materialization rejects an identity mismatch, so the corrupt object is not persisted, but it may already have caused additional remote object requests. A remote store that can serve arbitrary bytes under known CAS keys can therefore cause unnecessary dependency traversal and remote-read work before rejection.

Treat a configured remote object store as trusted for availability and object integrity. Limit its credentials and request budget accordingly.

### SSH flags can run commands on the local execution host

The SSH executor passes every supplied `flags` entry directly to the local `ssh` command. OpenSSH options such as `ProxyCommand` run a local command before the SSH connection is made.

For example, an SSH runnable with `flags=["-oProxyCommand=touch /tmp/dml-proxy-command"]` causes the local SSH client to run `touch /tmp/dml-proxy-command`. The command is run on the host executing DaggerML, not the remote SSH host.

Use only SSH flags you control. Do not pass untrusted values as SSH flags.
