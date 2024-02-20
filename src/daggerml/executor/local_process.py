#!/usr/bin/env python3
import asyncio
import inspect
import json
import socket
from tempfile import NamedTemporaryFile
from textwrap import dedent

import daggerml as dml


async def async_run_local_proc(dag, fn, *args, python_interpreter, preamble=(), cache: bool = True):
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
            f.write(dedent(f"""
            dml.set_flags({dml._util.CLI_FLAGS.flag_dict})
            dag = dml.Dag.from_state({fn_dag.dump_state()})
            """))
            f.write('\n')
            f.write(dedent(src))
            f.write('\n')
            f.write(dedent(f"""
            if __name__ == '__main__':
                try:
                    with dag:
                        result = {fn.__name__}(*dag.expr[1:])
                        if not isinstance(result, dml.Node):
                            result = dag.put(result)
                        result = dag.commit(result, cache={cache})
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    pass
                import json
                with open('{tmpb.name}', 'w') as f:
                    json.dump(dict(ref=result.ref.to, dag=result.dag.dump_state()), f)
            """))
        proc = await asyncio.create_subprocess_shell(
            f'{python_interpreter!r} {tmpa.name!r}',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        # subprocess.run([python_interpreter, tmpa.name], check=True)
        with open(tmpb.name, 'r') as f:
            tmp = json.load(f)
        dag = dml.Dag.from_state(tmp['dag'])
        node = dml.Node(ref=dml.Ref(tmp['ref']), dag=dag)
        return node


def run_local_proc(dag, fn, *args, python_interpreter, preamble=(), cache: bool = True):
    return asyncio.run(async_run_local_proc(dag, fn, *args,
                                            python_interpreter=python_interpreter,
                                            preamble=preamble,
                                            cache=cache))
