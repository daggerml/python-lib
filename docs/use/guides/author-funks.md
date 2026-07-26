# Author funks

This guide covers the current Python tooling: use `funkify` to package a Python function as a runnable funk. Script workers receive the function source and explicitly injected helpers, not module globals. Import dependencies inside the function body or provide `extra_objs` or `post_lines`.

```python
from daggerml import new
from daggerml.contrib import api

@api.funkify
def square(dag, number):
    return number.value() ** 2

with new("squares") as dag:
    result = dag.call(square, 9, name="result")
    dag.commit(result)
```

Funk arguments are node-like in the worker, so read input with `.value()`. Script-backed execution uses remote artifacts; configure `remote.root` before running it. Test author code with `daggerml.contrib.testing.defunkify` when a full runtime is unnecessary.
