# Dagger-ML Python Library

## Prerequisites

- [pipx](https://pypa.github.io/pipx/installation/)
- [hatch](https://hatch.pypa.io/latest/install/#pipx) (via `pipx`)

## Usage

See unit tests (or example) for usage.

## Example

Without any environments active, run:

```bash
hatch -e test-nevergrad run ./tests/example_optimization.py
```

Feel free to mess with the parameters, files, or defined functions in that file
to see how it affects the cache. You can also increase `budget` and watch the
dml cache in action.

Under the hood, the script uses the `nevergrad` library from the
`test-nevergrad` environment to optimize hyperparameters of a convolutional net
for image classification (hand-written digits).

## Tests

To run all tests:

```bash
hatch run pytest
```

Some of the tests (in particular the ones that build docker images) are super slow. To run all but those,

```bash
hatch run pytest -m 'not slow'
```
