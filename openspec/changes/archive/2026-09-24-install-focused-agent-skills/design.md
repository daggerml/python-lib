## Context

`_SkillsNamespace` currently reads package resources and returns strings; the generated CLI serializes them to stdout. The same public methods are callable in Python. The package resources already contain frontmatter with default names. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:** Keep method-driven CLI generation, install from package resources without a checkout, and preserve clear write diagnostics in both Python and CLI.

**Non-Goals:** Tool-specific registration, uninstall, or merging arbitrary files into existing skill directories.

## Decisions

- Use a required positional `parent: str` and optional `name: str | None`, `overwrite: bool = False` on each of the four methods; the CLI generates the flags and the methods return a concise diagnostic string containing the path and UTF-8 byte count. This retains the established generated surface rather than adding bespoke argparse commands.
- Share an installation helper that reads the bundled resource, validates a simple single-component slug for `name`, replaces only the frontmatter name, creates the parent, and writes `<parent>/<name>/SKILL.md`. An existing target directory is rejected unless `overwrite=True`; overwriting preserves unrelated files. Avoid replacing the directory wholesale.
- Keep bundled default names as packaged and substitute only the frontmatter name when requested. Reject path escapes and symlink targets when creating or overwriting files. On a failed write, do not return a success diagnostic.
- Expand guidance around practical workflows using the existing public docs as source of truth, removing the previous 1000-word/two-example limits where they obstruct useful explanations.
- Bundle each resource as `skills/<kind>/SKILL.md`, mirroring the installed directory layout and allowing additional supporting resources later. Include the nested Markdown files in source distributions and keep the authoring example self-contained with a dagclass definition and `api.run()` call.
- Put runnable examples in `skills/<kind>/examples/*.py` and let the skill document point to them relative to its installed directory. Recursively copy bundled skill files, preflight all destinations before overwriting, preserve unrelated files, and count the bytes written across the directory. Include example sources in both wheel and sdist.
- Use Vega's 2,000-flight airline-delay sample for the runnable dagclass example. Stage the source as a URI, keep train/test cuts and pickled models in the remote artifact store, run data/ML steps in a Docker image containing Polars and scikit-learn, and select the best trial using the out-of-sample R² node. Exercise the same schema against a deterministic fixture in a Docker/Moto integration test, building a test-owned image instead of depending on an externally maintained tag.
- Let the built-in Polars codec normalize returned train/test DataFrames to Parquet artifacts. Downstream funks read the codec-produced `s3://` URIs through Polars directly; when the worker runs against an S3-compatible endpoint, supply that endpoint to Polars explicitly. Keep S3Store only for the source JSON and pickled model.

## Risks / Trade-offs

- Existing stdout consumers break → Document migration to parent-directory installation and adjust contract tests.
- Replacing user-edited files can destroy work → Require explicit `--overwrite` for any existing skill directory, preserve unrelated files, and refuse symlink targets.
