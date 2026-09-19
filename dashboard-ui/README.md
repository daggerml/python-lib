# DaggerML Dashboard UI

This directory contains the React/TypeScript frontend for the local DaggerML
dashboard. Its production assets are built into `src/daggerml/dashboard/` so an
installed `dml-dashboard` does not require Node.js.

The UI consumes the dashboard's versioned local API. Normative route, API, and
product behavior is owned by the dashboard OpenSpec capabilities in
[`openspec/spec-overview.md`](../openspec/spec-overview.md).

For local UI development, run
`bash dashboard-ui/scripts/setup-dashboard-demo.sh` from the repository root.
It creates and registers two disposable projects with recent example history,
prints the command for starting the dashboard, and removes the fixture when
stopped with Ctrl-C.
