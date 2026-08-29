# Write a custom Python codec

This guide covers the current Python codec API. A Python codec converts a Python object to a value DaggerML can stage. Prefer a stable scalar, collection, `Uri`, or `Runnable` representation. For large data, upload the payload and return a `Uri`. Other language toolchains can define their own codecs for their values.

Codec registration is process-level plugin configuration through the `daggerml.codecs` entry-point group. Keep codec code deterministic: it participates in value normalization and therefore affects the durable graph and cache identity.

For dataframe-like values, the optional built-in pandas and polars codecs already write parquet artifacts through `S3Store`. Implementing and packaging shared codec plugins is covered by the extension documentation; this guide is for selecting or composing a codec in research code.
