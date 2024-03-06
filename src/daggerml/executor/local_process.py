#!/usr/bin/env python3
import asyncio
import inspect
import json
import socket
from tempfile import NamedTemporaryFile
from textwrap import dedent

import daggerml as dml


async def aio_run_local_proc(dag, fn, *args, python_interpreter, preamble=(), cache: bool = True):
    src = inspect.getsource(fn)
    resource = dml.Resource({'hostname': socket.gethostname(),
                             'function': {'source': src, 'name': fn.__qualname__},
                             'preamble': list(preamble),
                             'python-interpreter-path': python_interpreter})
    args = [x if isinstance(x, dml.Node) else dag.put(x) for x in args]
    fn_dag = dag.start_fn(dag.put(resource), *args)
    if cache and fn_dag.repo.cached_dag is not None:
        return fn_dag.commit(None)
    def get_tmpf(suffix):
        return NamedTemporaryFile(prefix=f'{__name__}-', suffix=suffix)
    with get_tmpf('.py') as tmpa, get_tmpf('.json') as tmpb:
        with open(tmpa.name, 'w') as f:
            for line in preamble:
                f.write(line + '\n')
            f.write('\nimport daggerml as dml\n')
            f.write(dedent("""
            """))
            f.write('\n')
            f.write(dedent(src))
            f.write('\n')
            f.write(dedent(f"""
            if __name__ == '__main__':
                try:
                    expr = {[dml._util.to_data(x.unroll()) for x in fn_dag.expr]!r}
                    expr = [dml._util.from_data(x) for x in expr]
                    result = {fn.__name__}(*expr[1:])
                    # result = expr
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    result = dml.Error.from_ex(e)
                import json
                with open('{tmpb.name}', 'w') as f:
                    json.dump(dml._util.to_data(result), f)
            """))
            f.seek(0)
        proc = await asyncio.create_subprocess_shell(
            f'{python_interpreter!r} {tmpa.name!r}',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            print(stderr)
        with open(tmpb.name, 'r') as f:
            tmp = json.load(f)
        resp = dml._util.from_data(tmp)
        if isinstance(resp, dml.Error):
            raise resp
        node = fn_dag.commit(fn_dag.put(resp))
        return node


def run_local_proc(dag, fn, *args, python_interpreter, preamble=(), cache: bool = True):
    return asyncio.run(aio_run_local_proc(dag, fn, *args,
                                          python_interpreter=python_interpreter,
                                          preamble=preamble,
                                          cache=cache))
