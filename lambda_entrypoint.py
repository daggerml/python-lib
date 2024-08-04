import os
import subprocess
import sys

tmpd = '/tmp/dml'
if not os.path.exists(tmpd):
    os.mkdir(tmpd)
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-t', tmpd, './submodules/daggerml_cli'])
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-t', tmpd, '.'])

os.environ['PYTHONPATH'] = f'{tmpd}:' + os.getenv('PYTHONPATH', '')
os.environ['PATH'] = f'{tmpd}/bin:' + os.getenv('PATH', '')
sys.path.append(tmpd)

def handler(event, context):
    import daggerml as dml
    with dml.Api(initialize=True) as api:
        dag = dml.new('exec', 'executing dag', dump=event['dump'], api_flags=api.flags)
        resp = dag.commit(dag.put(dag.expr))
        return {'message': 'qwer', 'response': dag.dump(resp)}
