# Codecs and values

Codecs are the bridge between ordinary Python values and the value model DaggerML stores in DAGs.

## The stored value model

At the storage layer, values are represented as datum objects such as:

- scalar datums for `str`, `int`, `float`, `bool`, and `None`
- list and dict datums that point to other datum refs
- `Uri` datums for external locations
- `RunnableDatum` for executable values

Nodes then point at those datums. A `LiteralNode` is the most direct example: it wraps a datum ref.

## What codecs do

Before a value is staged into a DAG, `daggerml.codecs` runs codec normalization.

The codec flow:

- finds the first matching codec by priority and registration order
- lets that codec encode the value
- re-applies codec matching if the encoded result changed
- recursively normalizes lists, dicts, and runnable fields

This happens on public DAG staging and call-entry paths, not deep inside storage.

## Built-in codec behavior

The built-in codecs cover two especially important cases:

- `NodeCodec` lets a node from the same DAG or another committed DAG be staged as a value
- `DelayedActionCodec` resolves delayed references, delayed loads, and delayed runnable construction into concrete staged values

That is how cross-DAG imports and adapter-driven runnable resolution can feel natural in Python while still landing in the explicit stored model.

## Plugin codecs

Additional codecs can be discovered through the `daggerml.codecs` entry-point group. The registry loads them lazily and applies them deterministically.

The practical mental model is simple: if a Python object is not already a plain stored value, a codec is the place where DaggerML learns how to turn it into one.

## How to think about it

Values enter DaggerML in Python shapes, but DAGs store a normalized graph of datum refs and node refs. Codecs are the translation layer that keeps those two worlds aligned.

See also:

- [DAGs and nodes](dags-and-nodes.md)
- [Execution](execution.md)
- [Storage](storage.md)
