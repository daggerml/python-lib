## 1. Remove clone command and internal entrypoints

- [x] 1.1 Remove `dml clone` CLI command wiring, argument parsing, and help text from `src/daggerml/_cli/**`.
- [x] 1.2 Remove clone orchestration methods from `src/daggerml/api.py` (`DmlOps`) and eliminate clone-specific internal API surfaces.
- [x] 1.3 Delete clone-only internal ops/modules and imports, refactoring shared helpers so remaining commands compile and run without clone branches.

## 2. Preserve and enforce init-first workflow

- [x] 2.1 Ensure `init` remains fully functional as the only bootstrap entrypoint, including recovery behavior for config-present/db-missing states.
- [x] 2.2 Remove clone-specific hook/config pathways (including post-clone handling) while preserving `post-init` behavior and environment contracts.
- [x] 2.3 Update remote/project initialization logic so no clone-origin recording path remains and explicit remote workflows (`fetch`/`checkout`/`pull`) are the only follow-up path.

## 3. Keep CLI as thin wrappers over internal APIs

- [x] 3.1 Refactor remaining project command handlers to parse inputs and call exactly one supported `daggerml._internal` API entrypoint per command.
- [x] 3.2 Remove any CLI-owned git-like orchestration code paths uncovered during clone removal.
- [x] 3.3 Verify CLI modules do not directly compose multi-step project workflows outside internal APIs.

## 4. Remove dead code and update tests/docs

- [x] 4.1 Remove or rewrite clone-focused tests across CLI/internal suites to assert clone absence and init + explicit remote command behavior.
- [x] 4.2 Remove clone references from user/developer docs and command examples; update workflow guidance to init-first.
- [x] 4.3 Run targeted and full test suites covering CLI routing, init, and remote operations; fix regressions caused by clone removal.
