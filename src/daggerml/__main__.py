import json
from argparse import ArgumentParser
from importlib import import_module

import daggerml as dml

if __name__ == '__main__':
    parser = ArgumentParser(description='run a python script as a dag')
    parser.add_argument('-m', '--module', help='', required=True)
    parser.add_argument('-f', '--fn-name', help='', required=True)
    parser.add_argument('-a', '--args-file', help='', required=True)
    parser.add_argument('-r', '--response-file', help='', required=True)
    args = vars(parser.parse_args())
    fn = getattr(import_module(args['module']), args['fn_name'])
    with open(args['args_file'], 'r') as f:
        expr = dml.from_json(f.read())
    try:
        result = fn(*expr)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        result = dml.Error(e)
    with open(args['response_file'], 'w') as f:
        json.dump(dml.to_data(result), f)
