from functools import partial
from inspect import getsource
from pathlib import Path
from textwrap import dedent
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from daggerml import Resource

_here_ = Path(__file__).parent
SCRIPT_EXEC = Resource("dml-script-exec", adapter="dml-local-adapter")


def update_query(resource, new_params):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    params.update(new_params)
    query = urlencode(params)
    new_uri = urlunparse(parsed._replace(query=query))
    out = Resource(new_uri, data=resource.data, adapter=resource.adapter)
    return out


def funkify(fn=None, base_resource=SCRIPT_EXEC, params=None, extra_fns=()):
    if fn is None:
        return partial(funkify, base_resource=base_resource, params=params, extra_fns=extra_fns)

    def get_src(f):
        lines = dedent(getsource(f)).split("\n")
        lines = [line for line in lines if not line.startswith("@funkify")]
        return "\n".join(lines)

    tpl = dedent(
        """
        #!/usr/bin/env python3
        import sys

        from daggerml import Dml

        {src}

        def _handler_(dump):
            with open(sys.argv[1], "w") as f:
                f.write(dump)

        if __name__ == "__main__":
            with Dml(data=sys.stdin.read(), message_handler=_handler_) as dml:
                with dml.new("test", "test") as dag:
                    res = {fn_name}(dag)
                    if dag._ref is None:
                        dag.result = res
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
    )
    resource = update_query(base_resource, {"script": src, **(params or {})})
    object.__setattr__(resource, "fn", fn)
    return resource
