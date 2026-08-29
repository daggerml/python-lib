# daggerml [![PyPI - Version](https://img.shields.io/pypi/v/daggerml.svg)](https://pypi.org/project/daggerml) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daggerml.svg)](https://pypi.org/project/daggerml)

DaggerML makes research computations durable, inspectable, cacheable DAGs. It records the inputs, functions, results, execution boundaries, and provenance behind a research result.

## Installation

Install [`daggerml`](https://github.com/daggerml/python-lib) in whichever [virtual environment](https://docs.python.org/3/tutorial/venv.html) you prefer.

```bash
pip install daggerml
```

Install terminal rendering support:

```bash
pip install "daggerml[terminal]"
```

## Start a project

```bash
mkdir research-demo && cd research-demo
dml init
```

Then author a DAG in Python:

```python
import daggerml as dml

with dml.new("first-result") as dag:
    result = dag.put(42, name="answer")
    dag.commit(result)
```

## Docs

- [Why DaggerML?](docs/why-daggerml.md)
- [Get started](docs/getting-started.md)
- [Use DaggerML](docs/use/README.md)
- [Extend DaggerML](docs/extend/README.md)
- [Develop DaggerML](docs/develop/README.md)

## Contributing

If you want to contribute, please check out the [contributing guide](CONTRIBUTING.md).

## License

`daggerml` is distributed under the terms of the [MIT](LICENSE) license.
