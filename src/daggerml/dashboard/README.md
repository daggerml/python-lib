# DaggerML Dashboard Server

This package serves the local dashboard, including its HTTP API, read model,
execution inspection, cancellation delegation, plugin results, and packaged
frontend assets. It consumes established DaggerML interfaces and remains
read-only except for confirmed cancellation through the runtime API.

The public dashboard plugin surface is exposed from `daggerml.dashboard`.
Normative dashboard behavior is owned by the dashboard OpenSpec capabilities in
[`openspec/spec-overview.md`](../../../openspec/spec-overview.md); the browser
source lives in [`dashboard-ui/`](../../../dashboard-ui/).
