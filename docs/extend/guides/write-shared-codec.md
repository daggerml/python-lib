# Write A Shared Python Codec

Use a shared Python literal codec for a Python type that should normalize consistently
across projects.

```python
class DecimalCodec:
    def can_encode(self, value):
        return isinstance(value, Decimal)

    def encode(self, value, dag):
        return str(value)


def codecs():
    return [(10, DecimalCodec())]
```

`encode()` receives the active `Dag`, so an external-data codec can write an
artifact and return a `Uri`. It must return a different type than the input and
should return a value that DaggerML can normalize, not a final node object.

Publish the factory, not the codec instance, under `daggerml.codecs`:

```toml
[project.entry-points."daggerml.codecs"]
my_package = "my_package.codecs:codecs"
```

Keep `can_encode()` narrow. Codec selection stops at the first match, ordered by
descending priority. Test both direct values and values nested in a list, dict,
`Uri`, or runnable kwargs when those shapes matter.
