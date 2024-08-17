import argparse
import sys
from uuid import uuid4

import daggerml as dml
import daggerml.executor as dml_ex

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ProgramName',
        description='What the program does',
        epilog='Text at the bottom of help')
    parser.add_argument('input_loc')
    parser.add_argument('output_loc')
    args = parser.parse_args()
    with dml.Api(initialize=True) as api:
        with open(args.input_loc, 'r') as f:
            dag = dml.new('new-dag', 'daggin-around', dump=f.read(), api=api)
        with dag:
            sh = dml_ex.Sh(dag)
            rsrc, script, _ = dag.expr
            rsrc = rsrc.value()
            assert isinstance(rsrc, dml.Resource)
            *_, fn_name = rsrc.id.split('/')
            script_name = f'dml_{uuid4().hex}'
            with sh.hydrate(**{f'{script_name}.py': script}) as tmpd:
                sys.path.append(tmpd)
                lib = __import__(script_name)
                resp = getattr(lib, fn_name)(dag)
                if not isinstance(resp, dml.Ref):
                    resp = dag.commit(resp)
            dump = api.dump(resp)
    with open(args.output_loc, 'w') as f:
        f.write(dump)
