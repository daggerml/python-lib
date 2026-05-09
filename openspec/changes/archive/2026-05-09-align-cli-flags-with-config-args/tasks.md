## 1. Parser And Handler Updates

- [x] 1.1 Rename top-level CLI override flags from `--repo` and `--remote-root` to `--project-home` and `--remote-uri` in `src/daggerml/_cli/__init__.py`.
- [x] 1.2 Update `src/daggerml/_cli/base.py`, `init.py`, `status.py`, and `config.py` so renamed parser destinations still resolve into canonical `project.home` and `remote.uri` values.
- [x] 1.3 Keep the public `--remote-uri` spelling in both top-level CLI parsing and `init`, but assign distinct argparse destinations so the two inputs do not collide.
- [x] 1.4 Update shared CLI error hints and command help text in `__init__.py`, `init.py`, `status.py`, and `remote.py` that still reference legacy flag names.

## 2. Documentation And Examples

- [x] 2.1 Update CLI examples and user-facing references in `docs/cli.md` and touched command epilog/help text to use `--project-home` and `--remote-uri`.
- [x] 2.2 Audit the repository for stale CLI references to `--repo` or `--remote-root` and replace the ones that describe the public CLI surface, including related internal error strings surfaced to CLI users.

## 3. Verification

- [x] 3.1 Update CLI contract tests to parse and execute the renamed flags, including coverage for top-level parser setup, `status`, `config`, and `init` entry points.
- [x] 3.2 Add test coverage for the dual `--remote-uri` surface so top-level overrides and `init --remote-uri` remain distinguishable.
- [x] 3.3 Update CLI integration tests that invoke init and full project lifecycle flows so they use the renamed flags throughout.
- [x] 3.4 Run the relevant CLI-focused test suite and confirm legacy flag references no longer appear in supported help or error guidance.
