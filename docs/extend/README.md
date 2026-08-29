# Extend DaggerML

This path is for integration engineers adding execution backends, adapters,
literal codecs, or packages that supply them. It documents extension contracts;
using a built-in integration does not require reading these pages.

## Start here

- [Extension model](concepts/extension-model.md): where delayed authoring,
  adapters, executors, codecs, and the runtime meet.
- [Write an adapter](guides/write-adapter.md): add a transport boundary.
- [Write an executor](guides/write-executor.md): add backend-specific work.
- [Plugin API](reference/plugin-api.md): package discovery and lookup rules.

## Browse by goal

- Concepts: [adapters and executors](concepts/adapters-and-executors.md),
  [codecs](concepts/codecs.md), [remote integrations](concepts/remote-integrations.md),
  and [plugin registration](concepts/plugin-registration.md).
- Guides: [shared codecs](guides/write-shared-codec.md),
  [package an integration](guides/package-integration.md), and
  [test an integration](guides/test-integration.md).
- Reference: [adapter operations](reference/adapter-operations.md),
  [executor lifecycle](reference/executor-lifecycle.md),
  [codec contracts](reference/codec-contracts.md), and
  [built-in integrations](reference/built-in-integrations.md).

The implementation namespace is `daggerml.contrib`, but this documentation is
organized by integration task rather than package name.
