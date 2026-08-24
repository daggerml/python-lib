## Why

The single generic coding-agent skill mixes DAG authoring, repository management, and inspection guidance, so agents receive irrelevant context and cannot retrieve focused help. Its `Dml.admin` home also misclassifies agent guidance as repository administration.

## What Changes

- **BREAKING** Replace the `Dml.admin` namespace with `Dml.skills`; do not provide a compatibility alias.
- **BREAKING** Replace `dml admin agent-skill` with focused generated CLI exports under `dml skills`.
- Bundle three portable Markdown skills as package resources under `src/daggerml/_core/skills/`: `authoring`, `repository`, and `inspection`.
- Make each exported skill concise, self-contained, and focused on its agent task, with only minimal examples and source-code pointers for deeper investigation.

## Capabilities

### New Capabilities
- `bundled-agent-skills`: Focused, portable agent-guidance documents for DaggerML authoring, repository management, and DAG/runtime inspection.

### Modified Capabilities
- `unified-dml-surface`: Replace the public `admin` namespace with `skills` and expose its skill-export methods.
- `admin-cli-controls`: Replace the administrative single-skill CLI export with generated `skills` subcommands.

## Impact

- Affects `src/daggerml/_core/dml.py`, packaged resource configuration, generated CLI help and command discovery, and `src/daggerml/SKILL.md`.
- Affects public Python and CLI API consumers; this is intentionally breaking during v0.
- Updates API/CLI contract tests, OpenSpec surfaces, and user-facing CLI/Python guidance.
