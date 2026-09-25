## Why

Focused agent guidance currently prints to stdout, leaving users to manually construct a skill directory and filename. Authoring and querying guidance also needs fuller workflows for agents working from an installed package.

## What Changes

- **BREAKING** Replace stdout exports with `dml skills <kind> <parent> [--name NAME] [--overwrite]`, installing `<parent>/<name>/SKILL.md` and returning a write diagnostic.
- Keep the four focused skill commands; allow custom installation names and explicit replacement of existing installations.
- Expand the authoring and querying skills with actionable examples and decision guidance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `admin-cli-controls`: Change the generated skill commands from stdout export to directory installation.
- `bundled-agent-skills`: Make authoring and querying guidance more complete and allow installed names to match chosen folder names.

## Impact

`src/daggerml/_core/dml.py`, bundled Markdown resources, generated CLI help, tests, and documentation/specs describing skill delivery. Existing stdout-redirection scripts must use the new destination argument.
