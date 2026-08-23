## Why

The Docker examples build unnecessarily large images by retaining compilers and other build-only packages in the runtime image, making example runs slower and less reliable. Their S3-backed `docker save` artifacts are also uploaded uncompressed, wasting transfer time and object storage.

## What Changes

- Build the example runtime image with a builder stage so build dependencies do not remain in the final image.
- Gzip Docker image tarballs before storing them in S3.
- Keep the ECR/registry branch of `docker_build` unchanged: it continues to tag and push the built image directly rather than creating or compressing a tarball.
- Add contract coverage for compressed artifact creation and the multi-stage example image, and document the S3 image artifact format.

## Capabilities

### New Capabilities

- `docker-image-artifacts`: Defines Docker image construction and publication behavior for compact example images, gzipped S3 image archives, and unchanged registry pushes.

### Modified Capabilities

None.

## Impact

- Affected implementation: `examples/dkr-ctx/Dockerfile` and `src/daggerml/contrib/funks.py`.
- Affected tests: contrib contract tests for the Docker build funk and example Dockerfile.
- Affected documentation: Docker workload and built-in integration references.
- External systems: S3 objects produced by `docker_build` use gzip-compressed Docker archives; registry/ECR pushes retain their current flow and image format.
- Public calling conventions and Docker executor code remain unchanged; Python tar auto-detection and `docker load` already accept gzip-compressed image archives.
