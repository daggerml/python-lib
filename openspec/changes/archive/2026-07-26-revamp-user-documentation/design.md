## Context

The existing docs tree is organized first by document type (`concepts`, `guides`, `reference`, and `architecture`) and then by package boundary (`contrib`). This makes researchers traverse internal terminology and intermixes authoring, runtime operation, adapter implementation, and core-contributor material.

DaggerML has three distinct readers: researchers who use DaggerML to create and manage research, integration engineers who extend it, and contributors who develop DaggerML itself. The docs must also establish the product rationale: DaggerML makes a research computation a durable graph of inputs, functions, results, execution boundaries, and provenance.

The CLI is the standard user surface for creating, configuring, inspecting, controlling, sharing, and cleaning up DaggerML projects. Python is the standard authoring surface for DAGs, funks, values, reusable prior work, and convenience APIs such as `temporary()`.

## Goals / Non-Goals

**Goals:**

- Provide a clear root-level explanation of why a researcher would use DaggerML.
- Organize documentation by the three reader paths: Use, Extend, and Develop.
- Give researchers one coherent path from `dml init` through authoring, runtime and cache control, sharing, reuse, and cleanup.
- Preserve technically valuable existing documentation by translating it into the appropriate reader path.
- Make the CLI/Python responsibility boundary consistent in examples and navigation.
- Make runtime, cache, and project-owned remote control discoverable as advanced researcher capabilities.

**Non-Goals:**

- Change DaggerML APIs, CLI commands, remote behavior, or runtime behavior.
- Add a separate platform-operator documentation audience while researchers manage their own project remotes.
- Treat every public Python method as a recommended user workflow.
- Replace code comments with product or contributor documentation.

## Decisions

### Use audience paths as the primary navigation

The root docs home will link to `why-daggerml.md`, `use/`, `extend/`, and `develop/`. Each audience path can use concepts, guides, and reference material internally, but those document types will not be the top-level navigation model.

This makes the first click answer the reader's question instead of exposing the repository's historical structure. Retaining the current top-level taxonomy would preserve familiar paths but would continue to make `contrib` and internal architecture appear relevant to first-time researchers.

### Keep researcher workflows CLI-first for project administration and Python-first for authoring

Researcher onboarding will initialize a project with `dml init`, configure and inspect it with `dml`, and use `dml` for history, remotes, runtimes, cache management, and cleanup. Python examples will begin from an existing project and use authoring helpers such as `new()`, `load()`, and `temporary()`.

`Dml.init(...)` can remain documented as a low-level public API where needed, but it will not be the recommended tutorial pattern. This avoids presenting two equally authoritative ways to create a project.

### Treat advanced research operation as part of Use DaggerML

The researcher path includes Docker image creation, supported remote execution choices such as SSH, external artifacts, custom codecs, runtime graph inspection and cancellation, and cache invalidation. A project remote can be an ephemeral S3 location created and controlled by the researcher, or a shared/managed location later.

Writing adapters, executors, plugin registrations, and shared integration contracts remains in Extend DaggerML. The distinction is whether the reader is composing supported capabilities into research or implementing those capabilities.

### Use reader terminology consistently

Researcher docs will use "runtime" for a live or inspectable computation in a DAG and explain its relationship to a DAG node. "Index" is an implementation term and will not be the primary user vocabulary. Cache guidance will describe researcher intent, such as refreshing a cached result, before naming low-level commands.

### Replace the contrib information architecture rather than hiding its content

`docs/contrib/` will not survive as a primary navigation subtree. Researcher-facing content currently there moves to `use/`; extension contracts and integration material move to `extend/`; internal wiring moves to `develop/`. Code import paths remain in examples and references where needed.

### Maintain a small, explicit target tree

The target tree is:

```text
docs/
  README.md
  why-daggerml.md
  glossary.md
  use/{README.md,getting-started.md,concepts/,guides/,reference/}
  extend/{README.md,concepts/,guides/,reference/}
  develop/{README.md,setup.md,testing.md,codebase-map.md,architecture/,contributing.md}
```

The detailed pages follow the agreed research lifecycle and extension/contributor concerns. Pages may be merged only when they do not answer distinct reader questions; the migration must not replace technical content with shallow navigation pages.

### Keep contributor material separate from automated workflow policy

`develop/` documents stable codebase concepts, setup, and contributor-facing architecture. Agent instructions, OpenSpec governance, edit maps, and other maintenance policy remain outside `docs/` in their current maintainer-oriented locations. This keeps the Develop path useful without turning product docs into agent workflow documentation.

## Risks / Trade-offs

- [A large rewrite loses useful technical detail] -> Inventory every current page and map it to a target page, an external maintainer document, or an explicit removal decision before deletion.
- [Audience paths duplicate shared concepts] -> Keep shared product terms in `glossary.md` and cross-link rather than duplicating definitions.
- [CLI-first guidance obscures useful Python administration APIs] -> Retain concise API reference coverage while labeling the CLI as the recommended researcher workflow.
- [Moving `contrib` pages breaks external links] -> Add redirects where the documentation host supports them; otherwise leave short migration stubs only when required by published links.
- [Researcher-owned remotes do not cover future managed deployments] -> Describe ephemeral, shared, and managed remotes as deployment modes without creating a new operator path.

## Migration Plan

1. Inventory current docs, examples, public APIs, CLI surfaces, and source modules by target audience.
2. Create root navigation and the Why DaggerML page before moving detailed content.
3. Rewrite the Use path, then the Extend and Develop paths, translating technical content into its new home.
4. Update repository README links and remove the obsolete top-level taxonomy and `contrib` subtree only after replacement pages exist.
5. Verify links, code examples, command names, terminology, and the final tree against the actual codebase.

The change is documentation-only. Rollback is a git revert of the documentation change; no data or runtime migration is required.

## Open Questions

- Which existing externally published documentation URLs require redirects rather than replacement links?
- Should cache invalidation retain its current `admin` CLI namespace while user guides frame it as an advanced research workflow, or should a later product change expose a researcher-oriented command alias?
