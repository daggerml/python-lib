## 1. Tighten Remote-Aware Signatures

- [x] 1.1 Identify remote-aware runtime and ops constructors/helpers that still model remote config as optional.
- [x] 1.2 Remove `Optional`, `| None`, and `None` defaults from required remote-root and remote-config parameters.
- [x] 1.3 Keep local-only code paths on local-only primitives instead of weakening remote-aware interfaces.

## 2. Update Call Sites

- [x] 2.1 Update in-repo runtime and ops call sites to pass explicit remote configuration wherever remote-backed behavior is used.
- [x] 2.2 Update test fixtures and helper scripts that currently construct remote-aware ops without remote config.
- [x] 2.3 Replace local setup uses of remote-aware ops with `BaseOps` or equivalent local-only primitives where no remote behavior is needed.

## 3. Verify Contract

- [x] 3.1 Update or add tests that assert remote-aware interfaces are constructed with explicit remote config.
- [x] 3.2 Run pyright and the relevant pytest coverage for remote-aware runtime and ops surfaces.
- [x] 3.3 Confirm no unsupported optional remote-config paths remain in the implementation.
