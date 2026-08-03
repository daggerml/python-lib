## Context

`dml` is generated from public `Dml` methods and annotated namespace properties. `_AdminNamespace` already supplies `dml admin` leaf commands, and `MethodCLI` emits a `str` result directly to standard output. The distribution currently packages Python sources but has no versioned, installable agent-facing Markdown resource.

## Goals / Non-Goals

**Goals:**
- Deliver one short, tool-neutral `SKILL.md` with standard `name` and `description` frontmatter.
- Expose it through the existing generated `dml admin` path with raw Markdown output suitable for shell redirection.
- Make the resource available from both built wheels and source distributions.
- Base every behavioral assertion in the skill on the repository's current public docs and examples.

**Non-Goals:**
- Installing the document into any agent tool's configuration directory.
- Supporting multiple skill formats, versions, locales, or command options.
- Replacing the user documentation or reproducing it in the skill.

## Decisions

### Package the skill as an internal Markdown resource

Place a single internal `SKILL.md` under the `daggerml` package and retrieve it with `importlib.resources`. Package-resource access works for installed wheels and does not rely on a source checkout or a filesystem-relative path. Configure the build explicitly to include the Markdown file in both wheel and sdist artifacts, then verify the built wheel contains and serves it.

Alternative: read a file from the repository `docs/` tree. Rejected because installed users may not have that tree and the resource would not necessarily match the installed library version.

### Add a zero-argument admin namespace method returning the resource text

Add `agent_skill()` to `_AdminNamespace` with a precise docstring and `str` return annotation. The existing generated command discovery yields `dml admin agent-skill`, and the existing string serializer preserves Markdown rather than JSON-encoding it. Because the generated CLI adds one terminal newline when printing a string result, the method removes only the resource's terminal newline before returning it; command output is byte-equivalent to the bundled document. It will not add labels or use a separate parser.

Alternative: add bespoke parsing to `_cli.py`. Rejected because it would bypass the generated public CLI contract and duplicate output handling.

### Keep the skill self-contained and compact

Use standard YAML frontmatter:

```yaml
---
name: daggerml
description: Concise guidance for coding agents working with DaggerML projects.
---
```

The body will have compact sections for environment and CLI orientation, DAG/node model with a `put`/`commit` example, funk execution with a `.value()` example and worker-isolation rule, dagclass composition, actionable sharp bits, and managed-project boundaries. Sharp-bit examples will show helper-source injection for cache correctness and forbid concurrent pull and administrative work. It will instruct agents to consult `dml --help`, public docstrings, and repository examples when available rather than duplicate broad documentation.

Alternative: OpenCode-specific layout or frontmatter. Rejected because users may install the exported Markdown into other coding-agent systems.

## Risks / Trade-offs

- [Skill text drifts from runtime behavior] → Keep the content short, base it on public docs/examples, and test its required guidance as content assertions.
- [Markdown is omitted from a release artifact] → Explicitly configure package data and test a built/installed wheel rather than only source-tree access.
- [CLI output is accidentally encoded or prefixed] → Test the subprocess command's exact output and redirectability.
- [Skill becomes a second long-form documentation surface] → Limit it to orientation, examples, and links/directions to command help, docstrings, and examples.

## Migration Plan

The command and bundled file are additive. Release them with the next package version; no project-state migration or rollback action is required. Removing the feature in a later release would require a documented CLI compatibility decision.
