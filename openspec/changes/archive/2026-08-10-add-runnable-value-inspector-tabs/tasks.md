## 1. Typed Runnable Read Model

- [x] 1.1 Add dashboard contract tests for server-authoritative node value classification, including scalar, error, ordinary Runnable, and FnNode-returned Runnable values.
- [x] 1.2 Add contract tests proving a function-applied runnable is exactly context DAG `argv[0]` and excludes Runnable arguments and runnables nested in prepopulation.
- [x] 1.3 Define the bounded runnable-inspection envelope for nested stack layers, entrypoint, script availability, prepopulation rows, truncation, and diagnostics.
- [x] 1.4 Implement node-value and function-applied runnable projection with exact outermost-to-innermost `sub` traversal and explicit malformed or unavailable evidence.
- [x] 1.5 Project script prepopulation as bounded name/type/optional-link rows, resolving applied links from the context DAG names map without serializing raw values.

## 2. Trusted Script Source Access

- [x] 2.1 Add read-model and HTTP contracts for node-value script reads covering readable script entrypoints, non-script entrypoints, missing URIs, unconfigured remotes, forbidden roots, missing objects, response bounds, and revision reachability.
- [x] 2.2 Implement the project-and-revision-scoped node-value script route by deriving the innermost script URI from the persisted node value and reusing configured-root and remote-descriptor validation.
- [x] 2.3 Return stable script availability and failure codes for function-applied and node-value runnable envelopes so the browser can distinguish every specified explanation.

## 3. Inspector Tabs and Shared Presentation

- [x] 3.1 Add frontend types and API hydration for explicit value classification, function-applied runnable envelopes, lazy scoped script reads, prepopulation rows, and bounded diagnostics.
- [x] 3.2 Add inspector routing tests proving every node has an addressable Value tab, only FnNodes and function-context DAGs have Runnable, and `tab=value` or `tab=runnable` restores without changing resource scope.
- [x] 3.3 Implement Value for bounded non-Runnable values and invoke the shared runnable presentation when the node value is a Runnable.
- [x] 3.4 Implement Runnable for FnNodes and function-context DAGs using the function-applied envelope, including the FnNode-returned-Runnable case where Value and Runnable show separate runnable data.
- [x] 3.5 Build the shared accessible runnable presentation for ordered stack layers, entrypoint identity, executor fields, script source or cause-specific explanation, truncation state, and prepopulation name/type/link rows.
- [x] 3.6 Remove value previews, runnable stacks, script source, and prepopulation details from Summary while retaining concise properties and context-DAG navigation.
- [x] 3.7 Add responsive styling, keyboard navigation, accessible labels, and node-link behavior for the new tabs and runnable presentation.

## 4. Documentation and Verification

- [x] 4.1 Update the normative dashboard architecture to define Value versus function-applied Runnable semantics, trusted node script reads, prepopulation links, bounds, and redaction.
- [x] 4.2 Run focused dashboard Python contracts, frontend tests, TypeScript type checking, lint or formatting checks, and the production frontend build.
- [x] 4.3 Rebuild packaged dashboard assets and visually verify ordinary values, ordinary Runnable values, FnNodes with scalar and Runnable results, wrapped script stacks, unavailable script reasons, and applied prepopulation links.
