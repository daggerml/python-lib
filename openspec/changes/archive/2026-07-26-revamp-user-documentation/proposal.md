## Why

The current documentation is organized around code and document types, which mixes researcher workflows, extension contracts, and DaggerML internals. DaggerML now needs a ground-up, audience-first documentation experience that explains its research value before directing readers to the level of detail they need.

## What Changes

- Replace the primary docs navigation with three reader paths: **Use DaggerML** for researchers, **Extend DaggerML** for integration engineers, and **Develop DaggerML** for core contributors.
- Add a root-level "Why DaggerML?" page that explains DaggerML as a way to make research computations durable, inspectable, cacheable, and versioned.
- Rewrite researcher documentation around the complete research lifecycle: project setup through the CLI, DAG and funk authoring in Python, execution environments, artifacts and codecs, runtime and cache control, failure inspection, and sharing or reusing research.
- Treat the CLI as the recommended surface for repository creation, configuration, inspection, runtime control, cache administration, history, and remote workflows; avoid presenting programmatic repository initialization as the normal researcher path.
- Preserve helpful Python authoring APIs, including temporary DML projects, in researcher-facing guidance and reference.
- Move adapter, executor, plugin, and shared-integration material into an explicit extension path; remove `contrib` as a primary reader-facing category.
- Separate core-contributor architecture and workflow material from product-user learning paths.
- Retire the existing top-level concepts/guides/reference/architecture/contrib navigation model and translate useful content into the new paths.

## Capabilities

### New Capabilities
- `researcher-documentation`: Audience-first documentation for authoring, running, controlling, sharing, and reproducing research with DaggerML.
- `extension-documentation`: Documentation for integration engineers who build adapters, executors, codecs, plugins, and supporting integrations.
- `contributor-documentation`: Documentation for contributors who develop DaggerML itself, including codebase architecture and contribution workflows.

### Modified Capabilities
- `human-facing-project-docs`: Change the primary information architecture and documentation voice from section-first technical documentation to audience-first product documentation.

## Impact

- Rewrites the `docs/` tree, root documentation links, and documentation navigation.
- Reclassifies and rewrites existing concepts, guides, reference pages, architecture pages, and the `docs/contrib/` subtree.
- Does not change DaggerML runtime behavior, public APIs, CLI behavior, dependencies, or package layout.
