# Package An Integration

Package an extension as an installable Python distribution with entry points.
The host process discovers installed entry points through `importlib.metadata`;
importing a module alone does not register it.

```toml
[project.entry-points."daggerml.contrib.adapters"]
my_transport = "my_package.adapter:MyAdapter"

[project.entry-points."daggerml.contrib.executors"]
my_backend = "my_package.executor:MyExecutor"
```

The entry-point key is descriptive only. Adapter lookup uses `MyAdapter.name`.
Executor lookup uses `(MyExecutor.adapter, MyExecutor.name)`. Ensure adapter
executables are installed and available on `PATH`: a runnable stores an
executable such as `dml-local-adapter`, and the runtime launches that executable
for adapter operations.

Install the package in the environment that authors DAGs and in every adapter
or worker environment that must lower or execute its runnables. Use
`daggerml.contrib.status.status()` for a JSON-safe view of discovered
registrations and plugin-load diagnostics; it is an inspection helper, not a
registration API.
