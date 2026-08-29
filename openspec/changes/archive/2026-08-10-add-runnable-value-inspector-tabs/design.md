## Context

See `proposal.md` for motivation and `specs/dashboard-value-runnable-inspection/spec.md` for the behavior contract. The current node response serializes a materialized value without an explicit value-type discriminator. FnNode and function-context DAG responses attach a `function` object built by recursively scanning the entire materialized argv value for runnables, selecting one resource heuristically, and exposing flattened chains and a script link in Summary. That scan can include unrelated Runnable arguments or values nested inside script prepopulation.

The script worker defines the authoritative relationship: the context DAG argv begins with the applied runnable, and execution follows that runnable's `sub` chain to `innermost()`. Script prepopulation is read from that innermost runnable and inserted as named nodes before invoking the rendered function. Script and log reads must remain bounded, revision scoped, derived from trusted persisted state, and constrained to the configured remote root.

## Goals / Non-Goals

**Goals:**

- Give the API and browser an explicit distinction between a node's persisted value and a function context's applied runnable.
- Reuse one typed runnable-inspection projection and one browser renderer for both Value and Runnable tabs.
- Resolve script and prepopulation evidence from the exact runnable being inspected.
- Preserve canonical inspector routing, revision reachability, redaction, and bounded reads.

**Non-Goals:**

- Change Runnable persistence, executor resolution, script rendering, cache identity, or execution behavior.
- Add arbitrary URI reads, remote object materialization, or fetching.
- Reconstruct an execution attempt or executor lifecycle from a persisted function DAG.
- Make uninstantiated prepopulation entries appear to be committed nodes.

## Decisions

### Use one typed runnable-inspection envelope in two semantic tabs

The read model will project Runnable evidence into one shape containing the nested runnable stack, an explicit innermost entrypoint summary, script availability and trusted link, bounded prepopulation rows, and truncation or diagnostic evidence. Value uses this envelope when the selected node's value is a Runnable. Runnable uses it for a function context's applied runnable.

The envelope will retain nested `sub` structure as the canonical representation because it matches persisted Runnable semantics. The browser may flatten it for ordered cards but will not infer stack membership by recursively searching arbitrary fields.

Alternative considered: retain the current `runnables` array. That array loses ownership and role, can include Runnable values from unrelated arguments or prepopulation, and cannot reliably distinguish the applied stack from nested data.

### Make value typing server authoritative

Node responses will include an explicit value classification alongside the bounded value projection. A Runnable value will carry a runnable-inspection envelope derived while the server still knows the materialized Python type. Non-Runnable values retain the existing bounded JSON/error projection used by Value.

Alternative considered: detect Runnable-shaped objects in TypeScript by checking `kind`, `target`, and `sub`. Arbitrary dictionaries can share those keys, so shape inference would make tab content depend on accidental user data.

### Derive the applied runnable only from context DAG argv[0]

Function context projection will materialize the persisted argv, require a sequence whose first item is a Runnable, and project only that first item as the applied runnable. Other Runnable arguments remain values; runnables inside `prepop` do not become stack layers. FnNode and context-DAG reads will reuse the same projection keyed by the context DAG ref.

Alternative considered: keep recursively scanning all argv values and select the first adapter-bearing item. This is ambiguous and can display a dependency runnable rather than the runnable that produced the FnNode.

### Derive script reads from scoped resource identities

The existing function-DAG script route remains the trusted source for the applied Runnable tab. A node-scoped value-script route will accept project, concrete revision, and node ref; validate node reachability using the same rules as node inspection; rematerialize that node's value; require a Runnable; follow `sub` to the innermost entrypoint; and read its persisted `script_uri` only through the existing configured-root and remote-descriptor checks.

The runnable-inspection envelope will describe script availability before a read: non-script entrypoint, missing URI, or eligible trusted link. The browser will map route failures such as unconfigured remote, forbidden root, and missing object to cause-specific bounded messages instead of swallowing them.

Alternative considered: send the sanitized URI back to a generic script endpoint. Even a sanitized client value is not an authorization boundary and would permit arbitrary resource probing.

### Project prepopulation rather than serializing raw values

The innermost script runnable's `prepop` mapping will become bounded rows containing name, safe type, and optional node link. For an applied function context, links are resolved by matching prepopulation names against the context DAG's persisted names map and validating the linked nodes in the same revision. For a Runnable node value, prepopulation has not necessarily been instantiated, so rows have no node link unless persisted scoped evidence proves one exists.

Raw prepopulation values will not enter the response. This preserves the established redaction boundary and prevents large nested values or embedded runnables from expanding the payload.

Alternative considered: expose a bounded JSON preview. The agreed product surface needs name, type, and link, and previews add disclosure risk without improving navigation.

### Keep tab meaning stable and route-addressable

Every node inspector advertises Value. FnNodes additionally advertise Runnable through their function context. A function-context DAG advertises Runnable. Other DAGs do not. The labels never change based on value type: a Runnable returned by an FnNode remains under Value, while the applied `argv[0]` remains under Runnable. Both tabs call the same runnable component with separate envelopes.

Tab order is Summary, Value, Runnable, DAG, Logs, and Resources where those tabs apply. Summary retains concise properties and context navigation but no value preview, runnable stack, script source, or prepopulation table.

Alternative considered: rename Value to Runnable for Runnable-valued nodes. Conditional names obscure the stable distinction between stored value and applied function.

## Risks / Trade-offs

- [Materializing node values can be expensive] → Reuse the existing bounded node read, avoid duplicate reads within one request, cap nested projection depth and item counts, and fetch script bytes only when the user opens the relevant tab.
- [Context DAG names may not contain every prepopulation entry] → Make node links optional and label absent links as not instantiated or unavailable without guessing.
- [Script-route failures have multiple causes] → Use stable availability or error codes in the response and map them to explicit user-facing explanations.
- [Changing the internal v0 node/function response shape affects the packaged browser] → Update server and frontend atomically with contract tests; no legacy response adapter is required for this internal v0 API.
- [A Runnable may have malformed or cyclic mapping data] → Enforce depth bounds, track visited nested objects where applicable, and return truncated or malformed diagnostics rather than recursing indefinitely.

## Migration Plan

1. Add the typed runnable and value classifications while keeping existing node and function reads operational.
2. Add trusted node-value script access and focused security/revision contracts.
3. Switch the browser to Value and Runnable tabs and remove duplicate Summary rendering.
4. Update architecture documentation and rebuild packaged frontend assets.
5. Remove obsolete ambiguous `runnables`/`resources` function-context fields once all dashboard consumers use the typed envelope.

Rollback is an application rollback: the change does not migrate persisted data or alter public Python APIs.
