## Purpose

Defines compact Docker image production and portable S3 image artifacts that execute identically to directly referenced registry images.

## ADDED Requirements

### Requirement: Example runtime images exclude build-only dependencies
The example Docker image SHALL use a separate build environment and SHALL omit compilers, source-control tools, build-system packages, and repository source that are not required to execute the installed example workloads from its final runtime stage.

#### Scenario: Build dependencies stay outside the runtime image
- **WHEN** the example Dockerfile builds the DaggerML package and its example dependencies
- **THEN** the final image contains the installed runtime environment without retaining the builder toolchain or repository checkout

### Requirement: S3 image artifacts are gzip-compressed
When Docker image construction targets S3 artifact storage, the build helper SHALL serialize the image as a gzip-compressed Docker image tar archive and SHALL return the URI of that compressed object.

#### Scenario: Build without a registry destination
- **WHEN** a Docker image build completes without a registry repository argument
- **THEN** the helper uploads a gzip-compressed Docker image archive to S3 and returns its URI

### Requirement: Registry publication remains direct
When Docker image construction targets a registry repository, the build helper SHALL tag and push the built image directly and SHALL NOT create or upload an S3 image archive as part of that publication path.

#### Scenario: Build with an ECR repository destination
- **WHEN** a Docker image build completes with an ECR repository argument
- **THEN** the helper pushes the image to ECR and returns the registry image URI without creating a gzip archive
