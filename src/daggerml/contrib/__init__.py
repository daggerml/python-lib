import re
from functools import partial
from inspect import getsource
from pathlib import Path
from textwrap import dedent
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from daggerml import Resource

_here_ = Path(__file__).parent
SCRIPT_EXEC = Resource("dml-script-exec", adapter="dml-local-adapter")
DOCKER_EXEC = Resource("dml-docker-exec", adapter="dml-local-adapter")


def parse_query(resource):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    return params


def update_query(resource, new_params):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    params = {k: v[0] for k, v in params.items()}
    params.update(new_params)
    query = urlencode(params, doseq=True)
    new_uri = urlunparse(parsed._replace(query=query))
    out = Resource(new_uri, data=resource.data, adapter=resource.adapter)
    return out


def funkify(fn=None, base_resource=SCRIPT_EXEC, params=None, extra_fns=(), extra_lines=()):
    if fn is None:
        return partial(funkify, base_resource=base_resource, params=params, extra_fns=extra_fns)

    def get_src(f):
        lines = dedent(getsource(f)).split("\n")
        lines = [line for line in lines if not re.match("^@.*funkify", line)]
        return "\n".join(lines)

    tpl = dedent(
        """
        #!/usr/bin/env python3
        import os
        from urllib.parse import urlparse

        from daggerml import Dml

        {src}

        {eln}

        def _get_data():
            indata = os.environ["DML_INPUT_LOC"]
            p = urlparse(indata)
            if p.scheme == "s3":
                import boto3
                data = (
                    boto3.client("s3")
                    .get_object(Bucket=p.netloc, Key=p.path[1:])
                    ["Body"].read().decode()
                )
                return data
            with open(indata) as f:
                return f.read()

        def _handler(dump):
            outdata = os.environ["DML_OUTPUT_LOC"]
            p = urlparse(outdata)
            if p.scheme == "s3":
                import boto3
                data = (
                    boto3.client("s3")
                    .put_object(Bucket=p.netloc, Key=p.path[1:], Body=dump.encode())
                )
            with open(outdata, "w") as f:
                f.write(dump)

        if __name__ == "__main__":
            with Dml(data=_get_data(), message_handler=_handler) as dml:
                with dml.new("test", "test") as dag:
                    res = {fn_name}(dag)
                    if dag._ref is None:
                        dag.result = res
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
        eln="\n".join(extra_lines),
    )
    resource = update_query(base_resource, {"script": src, **(params or {})})
    object.__setattr__(resource, "fn", fn)
    return resource


@funkify
def dag_query_update(dag):
    from daggerml import Resource
    from daggerml.contrib import update_query

    old_rsrc, params = dag.argv[1:].value()
    params = {k: v.uri if isinstance(v, Resource) else v for k, v in params.items()}
    dag.result = update_query(old_rsrc, params)


@funkify
def dkr_build(dag):
    from daggerml.contrib.dkr import dkr_build

    tarball = dag.argv[1].value()
    flags = dag.argv[2].value() if len(dag.argv) > 2 else []
    dag.info = dkr_build(tarball.uri, flags)
    dag.result = dag.info["image"]


@funkify
def dkr_push(dag):
    from daggerml import Resource
    from daggerml.contrib.dkr import dkr_push

    image = dag.argv[1].value()
    repo = dag.argv[2].value()
    if isinstance(repo, Resource):
        repo = repo.uri
    dag.info = dkr_push(image, repo)
    dag.result = dag.info["image"]
