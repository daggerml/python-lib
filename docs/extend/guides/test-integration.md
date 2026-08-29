# Test An Integration

Test contracts before infrastructure.

1. Unit-test `resolve_runnable()` validation and the exact runnable it returns.
2. Call `ExecutorBase.handle()` with `adapter_state=None`, saved state,
   `operation="cleanup"`, and `operation="cancel"` to verify start, internal
   poll, cleanup, and cancel routing. Assert `operation="poll"` is rejected.
3. Test adapter request and response JSON, including the cancel-only fields
   `requested_by` and `argv_ref`, plus cleanup's required `result_ref`.
4. Test entry-point discovery in an isolated registry state, including missing
   and duplicate registrations.
5. Add a slow integration test for real transport, remote storage, worker, or
   polling behavior.

Cover retry state and delay hints, repeated cleanup, cleanup failure without
lifecycle corruption, nested wrapper cleanup, and cancellation teardown.

The repository's extension tests live under `tests/contrib/contracts/` and
`tests/contrib/integration/`. Follow the contributor test naming and marker
policy in [`CONTRIBUTING.md`](../../../CONTRIBUTING.md); infrastructure-dependent tests need
`@pytest.mark.slow`.

For a script-backed delayed runnable, `daggerml.contrib.testing.defunkify()`
can execute the innermost retained callable in a temporary directory. It is not
a runtime emulator: it only supports an innermost `script` runnable with a
callable still present in `kwargs["fn"]`.
