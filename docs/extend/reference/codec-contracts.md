# Python Codec Contracts

This reference defines the current Python codec API. It does not define a cross-language codec protocol; other language toolchains can provide their own value-normalization mechanisms.

The public protocol in `daggerml.api` is:

```python
class LiteralCodec(Protocol):
    def can_encode(self, value: Any) -> bool: ...
    def encode(self, value: Any, dag: Dag) -> Any: ...
```

`can_encode()` is evaluated in registration order after priority sorting.
Exceptions from a codec are wrapped as `CodecError`, except `DmlRepoError`,
which is propagated. `encode()` must not return `type(value)`; doing so raises
`CodecError` because normalization would not progress.

Register codec factories in `daggerml.codecs`. A factory returns an iterable of
`(priority: int, codec)` pairs. Loading occurs once per process on first codec
use. Entry-point load or factory failures raise `CodecError` and do not mark
plugins loaded, so a future use can retry.

`apply_codecs()` recursively normalizes list and dictionary elements, URI text,
runnable target/sub/kwargs, and delayed extension values. A codec can return a
`Uri`, `Runnable`, DaggerML literal, collection, reference, or another
encodable intermediate type, provided the chain eventually terminates.
