## 1. Compressed S3 Artifact Production

- [x] 1.1 Add contract coverage for actual gzip output, the `.tar.gz` S3 upload, and the registry branch's early return before archive creation.
- [x] 1.2 Update the no-registry branch of `docker_build` to gzip the `docker save` archive with explicitly injected, self-contained code and upload the compressed artifact, leaving tag-and-push behavior unchanged.

## 2. Smaller Example Runtime Image

- [x] 2.1 Convert `examples/dkr-ctx/Dockerfile` to a builder stage that installs into a fixed-path virtual environment and a slim runtime stage that copies only that environment.
- [x] 2.2 Add a fast example contract asserting the final Dockerfile stage does not install or copy the build toolchain and repository source.

## 3. Documentation And Verification

- [x] 3.1 Update the Docker workload guide and built-in integration reference to describe gzip-compressed S3 image artifacts and unchanged registry pushes.
- [x] 3.2 Run the targeted contrib and example contract tests covering Docker image production and structure.
- [x] 3.3 Run the required typecheck, lint-fix, and non-slow test suite; run the Docker example end to end when Docker and its required services are available.
