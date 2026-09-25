## 1. Install focused skills

- [x] 1.1 Replace four export methods with directory-installing methods using shared safe name validation, frontmatter substitution, byte-count diagnostics, and overwrite semantics.
- [x] 1.2 Update contract tests for generated CLI syntax, default/custom names, existing targets, overwrite, and rejected unsafe names.

## 2. Expand guidance and document migration

- [x] 2.1 Expand bundled authoring and querying skills with self-contained workflows and correct examples.
- [x] 2.2 Update CLI documentation and current OpenSpec contracts for installed skills; adjust DOC_MAP if needed.

## 3. Verify

- [x] 3.1 Run focused tests and required typecheck, lint-fix, and non-slow tests; resolve attributable failures.

## 4. Package skill directories

- [x] 4.1 Move each bundled resource to `skills/<kind>/SKILL.md`, update installer lookup and packaging, and verify the built distributions contain all four documents.
- [x] 4.2 Include a complete dagclass definition and invocation in the authoring skill, and update resource tests and spec contracts.
- [x] 4.3 Rerun typecheck, lint-fix, and non-slow tests.

## 5. Include runnable supporting examples

- [x] 5.1 Move the dagclass example into `authoring/examples/dagclass.py` and link to it from the authoring skill.
- [x] 5.2 Install all bundled skill files, handle overwrite safely, and include the Python example in built distributions.
- [x] 5.3 Update contracts and documentation, then run required checks.

## 6. Exercise a real modeling workflow

- [x] 6.1 Replace the trivial dagclass example with an airline-delay tree search, artifact-backed model and dataset cuts, and printed best parameters and score.
- [x] 6.2 Build a Docker image with Polars and scikit-learn in a deterministic integration test and verify the committed search result.
- [x] 6.3 Update skill guidance and specifications, then run focused and required checks.

## 7. Use built-in Polars codecs

- [x] 7.1 Return DataFrames from preparation and have training, prediction, and metrics read codec-produced Parquet URIs directly with Polars.
- [x] 7.2 Check direct Polars reads against Moto and rerun the Docker model-search test when the daemon is available.
- [x] 7.3 Update skill guidance and run required validation.
