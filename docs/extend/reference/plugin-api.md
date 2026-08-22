# Plugin API

## Adapter and executor entry points

```toml
[project.entry-points."daggerml.contrib.adapters"]
example = "my_package.adapter:ExampleAdapter"

[project.entry-points."daggerml.contrib.executors"]
example = "my_package.executor:ExampleExecutor"
```

The adapter entry point must load an object with `name`, `resolve_runnable`,
`send`, and normally `cli`. The executor entry point must load an object with
`adapter`, `name`, `resolve_runnable`, `start`, `poll`, `cleanup`, and `cancel`.
`poll` is the internal status method selected by repeated invoke requests, not a
wire operation. Cleanup accepts published-result context and must be idempotent.
Classes are appropriate because `ExecutorBase.handle()` instantiates them.

`get_adapter(name)` and `list_adapters()` trigger adapter discovery.
`get_executor(adapter, name)` and `list_executors(adapter)` trigger executor
discovery. Discovery loads each entry point once per process and stores the
loaded object directly; there is no public `register_adapter()` or
`register_executor()` call, and plugin entry points must not return an iterable
or callback factory.

Duplicate adapter names and executor `(adapter, name)` pairs emit warnings and
overwrite the previous object. A failed entry point raises `DmlRepoError` with
its entry-point name and value.

## Codec entry points

```toml
[project.entry-points."daggerml.codecs"]
example = "my_package.codecs:literal_codecs"
```

Unlike adapter and executor plugins, this entry point loads a zero-argument
callable and calls it. Its return value is an iterable of `(priority, codec)`
