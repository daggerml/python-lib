## Context

Bundled skills are Markdown package resources exposed through matching methods on `Dml.skills`, which also generate `dml skills` subcommands. The current contract fixes three resource names and caps each document at 250 words. The content is therefore both an API surface and portable guidance consumed independently of this repository.

## Goals / Non-Goals

**Goals:**

- Align each skill with one coherent agent task.
- Keep all four exports self-contained while allowing enough space for accurate examples and operational constraints.
- Make tests verify durable content boundaries without freezing prose unnecessarily.

**Non-Goals:**

- Preserve `inspection` as an alias.
- Teach DaggerML internals unrelated to the selected task.
- Establish a permanent target length below the 1000-word maximum.

## Decisions

### Use four task-oriented exports

The export set becomes `querying`, `authoring`, `repository`, and `extensions`. `inspection` is replaced rather than retained because aliases would preserve an obsolete boundary and create a fifth apparent skill.

Alternative considered: retain `inspection` and add only `extensions`. This leaves data access mixed with execution and cache operations, contrary to the desired task model.

### Keep querying read-oriented

`querying` covers locating DAG data, selecting terminal and named nodes, projections, materialization, provenance traversal, and persisted error capture. It may explain state distinctions needed to read committed or partial graphs, but does not own cache validation, invalidation, synchronization, or history mutation.

### Put cache operations with repository state

`repository` owns setup, configuration, history, refs, remotes, dependencies, garbage collection, and cache inspection/control. Cache entries and exact execution refs are shared repository/remote state; validation and invalidation therefore fit the same inspect-before-mutate discipline as synchronization and GC.

### Teach node preservation before materialization

`authoring` explicitly directs agents to pass nodes, projections, required results, and call results directly into funks and nested graph structures. Its example contrasts direct node passing at the authoring boundary with `.value()` inside a funk where ordinary Python computation requires concrete data. This avoids examples that accidentally teach provenance loss.

### Treat extensions as the integration umbrella

The fourth skill is `extensions`, not `adapters`. It distinguishes adapters as transport, executors as backend lifecycle, and codecs as staging normalization, then covers plugin registration and contract-first testing. This prevents agents from placing backend behavior in the adapter layer.

### Enforce a ceiling, not a target

Contract tests allow at most 1000 words per resource and continue checking portable frontmatter, bounded examples, and topic-specific guidance. The documents can be pruned below that limit without changing the contract.

## Risks / Trade-offs

- [Removing `inspection` breaks existing commands and callers] -> Document `querying` as the direct migration and fail normally for the removed command.
- [Repository becomes broad] -> Keep cache material limited to inspection, exact-ref validation, invalidation, and safety; leave execution implementation to extension/runtime documentation.
- [Larger documents accumulate cruft] -> Treat 1000 words as a hard maximum and test required concepts rather than exact prose.
- [Topic overlap reappears] -> Assign read-only graph/error work to `querying`, DAG construction to `authoring`, shared state/control to `repository`, and integration contracts to `extensions`.

## Migration Plan

Replace the resource and export method atomically, add the fourth resource and method, then update contract tests and public documentation in the same release. Consumers invoking `skills inspection` migrate to `skills querying`; no compatibility alias is retained.
