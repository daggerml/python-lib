# Agent Instructions

- Before editing code, consult relevant docs via `docs/DOC_MAP.md`.
- In your final summary/PR notes, list the docs you consulted.
- When a function runs through the script executor (`@api.funkify(uri="script", ...)`), only the function source and explicitly injected `extra_objs`/`extra_lines` are available in the worker; module-level imports/globals are not.
- Keep script-executed functions self-contained: import dependencies inside the function body or inject them explicitly, or runtime `NameError` failures can appear at the call site.
