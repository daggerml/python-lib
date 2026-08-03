## Why

Coding agents installed alongside DaggerML lack a compact, version-matched orientation to its authoring model and CLI. Users need a supported way to retrieve that guidance directly from an installed package without locating this source repository.

## What Changes

- Bundle a concise, portable agent skill Markdown document with the DaggerML distribution.
- Add `dml admin agent-skill`, which writes the complete skill document to standard output for redirection into an agent's skill location.
- Define the document's standard Markdown frontmatter and its minimum guidance: environment and CLI use, DAG/node authoring, funks, script-worker isolation, sharp bits, remote requirements, and safe project boundaries.
- Document the command in the CLI reference.

## Capabilities

### New Capabilities
- `agent-skill-distribution`: Delivers a portable, version-matched DaggerML coding-agent skill from an installed package.

### Modified Capabilities
- `admin-cli-controls`: Adds the agent-skill export command to the `dml admin` command group.

## Impact

- Affects the public `dml` CLI, the `Dml` administration namespace, package resource configuration, CLI documentation, and CLI/package tests.
- Adds no runtime dependencies and does not alter existing commands.
