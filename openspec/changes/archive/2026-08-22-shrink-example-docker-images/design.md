## Context

See `proposal.md` for motivation. The example image currently installs the native build toolchain and project source into its only stage. The script-executed `docker_build` helper saves a local image to an uncompressed tar before S3 upload, while its registry branch tags and pushes without creating an archive. `DockerExecutor` already opens archives with Python's transparent compression detection and passes them to `docker load`, which accepts gzip-compressed image archives.

The build helper runs through the script executor, so every dependency it uses must be imported inside the function or supplied through `extra_objs`. Existing uncompressed S3 image artifacts are durable DAG values and must remain loadable.

## Goals / Non-Goals

**Goals:**

- Keep build-only operating-system packages and project source out of the final example image.
- Store newly built S3 image artifacts as gzip-compressed Docker archives.
- Leave the Docker executor and registry publication branch unchanged.
- Verify archive behavior with fast, isolated contrib contract tests.

**Non-Goals:**

- Any Docker executor implementation changes.
- Compressing or otherwise modifying images pushed to ECR or another registry.
- Introducing a general archive-format option or changing `S3Store.tar()` behavior.
- Optimizing third-party runtime dependencies such as pandas, scikit-learn, PyArrow, or s3fs out of the example image.

## Decisions

### Use a multi-stage Dockerfile with a copied virtual environment

The builder stage will install the native build packages, copy the repository, create a virtual environment, and install DaggerML plus the example runtime dependencies into it. The final stage will start from the same slim Python base, copy only that virtual environment, put it on `PATH`, and retain the existing runtime environment settings.

This keeps package installation straightforward and preserves console scripts such as the adapter executable. Copying a wheel and reinstalling dependencies in the final stage was considered, but it would add a second dependency-resolution/install pass and more Dockerfile machinery without reducing the resulting Python dependency set.

### Compress only after the no-registry build branch is selected

The no-registry branch will produce a normal `docker save` archive, gzip it with Python standard-library support, upload the `.tar.gz` file, and let the temporary build directory clean up both files. Imports for compression and file copying will remain inside `docker_build` because it executes in an isolated script worker.

Streaming `docker save` directly through a shell pipeline was considered, but the existing command helper intentionally avoids a shell and exposes no stream plumbing. Extending that helper solely for this optimization would increase the change surface. A standard-library compression pass is simpler and keeps command error reporting unchanged.

### Test production and image structure at their contract boundaries

The build helper contract will verify actual gzip output and assert that the rendered no-registry branch uploads a `.tar.gz` artifact while the registry branch still returns before archive creation. A static Dockerfile contract will verify that build packages and repository source remain confined to the builder stage.

A full example image build remains an integration concern because it requires Docker, network/package installation, and significant runtime. The Dockerfile stage structure can be checked statically in a fast contract test if existing rendered-script assertions do not adequately cover it.

## Risks / Trade-offs

- [Compression adds CPU time and temporarily retains compressed and uncompressed files] -> Keep both files inside the existing temporary directory and prefer the storage/transfer reduction for large artifacts; revisit streaming only if local disk usage becomes a measured problem.
- [A copied virtual environment can encode its original absolute path] -> Create and copy it at the same absolute path in both image stages.
- [Docker implementations could vary in compressed archive support] -> Exercise `docker load` in the maintained example/integration path; no executor implementation changes are needed for currently supported Docker versions.
- [Changing new artifact suffixes can affect assumptions outside the executor] -> Keep image values as opaque S3 URIs and document `.tar.gz`; the executor detects content rather than requiring the suffix.

## Migration Plan

1. Update S3 artifact production to emit `.tar.gz` while retaining the registry branch unchanged.
2. Convert the example Dockerfile to a builder and runtime stage and run the Docker example where Docker is available.
3. Update Docker workload and built-in integration documentation to describe compressed S3 artifacts.

Rollback can restore uncompressed production without data migration because the executor continues to accept both formats. Existing S3 artifact URIs require no rewrite.
