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
    proc = subprocess.run(['./dml_script'], input=event['dump'].encode(), capture_output=True, check=False)
    return {'status': proc.returncode, 'stdout': proc.stdout.decode(), 'stderr': proc.stderr.decode()}
