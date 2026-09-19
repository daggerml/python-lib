# Codecs

This page documents the current Python codec contract. A literal codec
translates a Python value into a value DaggerML can stage. Other language
toolchains can define their own codec and normalization mechanisms. A Python
codec implements `can_encode(value)` and `encode(value, dag)`.

`apply_codecs()` repeatedly applies codecs until it has a DaggerML scalar,
collection, `Error`, or `Ref`, then recursively normalizes collections,
`Uri`, and `Runnable` fields. `encode()` must therefore make progress: returning
the same Python type is rejected as a `CodecError`.

Codec plugins use the `daggerml.codecs` entry-point group. Each entry point
loads a zero-argument factory that returns an iterable of `(priority, codec)`
pairs. Higher priority runs first; equal priorities preserve registration order
after entry points are sorted by name and value.

The built-in contrib dataframe codecs externalize pandas or polars dataframes
as Parquet through `S3Store`, returning a `Uri`. They are available only when
their optional dataframe library imports successfully.
