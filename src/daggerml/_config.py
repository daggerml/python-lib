import logging
import os
import pathlib
import re
import sys
from collections.abc import MutableMapping as Map
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
from urllib.parse import urlparse


logger = logging.getLogger(__name__)
USER_HOME_DIR = str(pathlib.Path.home())

LOCALSTACK_HOST = os.getenv('LOCALSTACK_HOST')
EDGE_PORT = os.getenv('EDGE_PORT', '4566')
AWS_LOCALSTACK_ENDPOINT = None

DML_ZONE = os.getenv('DML_ZONE')
DML_REGION = os.getenv('DML_REGION')
DML_GROUP_ID = os.getenv('DML_GROUP_ID')
DML_API_ENDPOINT = os.getenv('DML_API_ENDPOINT')
DML_API_KEY = os.getenv('DML_API_KEY')
DML_PROFILE = os.getenv('DML_PROFILE')


def deep_merge(d, v):
    for key in v:
        if key in d and isinstance(d[key], Map) and isinstance(v[key], Map):
            deep_merge(d[key], v[key])
        else:
            d[key] = v[key]


def configure():
    keys = {
        'config': {
            'group_id': 'DML_GROUP_ID',
            'api_endpoint': 'DML_API_ENDPOINT',
        },
        'credentials': {
            'api_key': 'DML_API_KEY',
        },
    }

    config_dirs = [
        os.getcwd(),
        USER_HOME_DIR,
    ]

    config_files = [
        'credentials',
        'config',
    ]

    profiles = [
        DML_PROFILE,
        'DEFAULT',
    ]

    def set_global(name, value):
        if globals()[name] is None:
            globals()[name] = value

    def from_file(config_dir, file_type, profile):
        config_file = os.path.join(config_dir, file_type)
        ks = keys[file_type]
        if os.path.exists(config_file) and profile is not None:
            config = ConfigParser(interpolation=ExtendedInterpolation())
            config.read(config_file)
            if profile in config:
                section = config[profile]
                for (k, v) in section.items():
                    if k in ks:
                        set_global(ks[k], v)

    for d in config_dirs:
        for f in config_files:
            for p in profiles:
                from_file(os.path.join(d, '.dml'), f, p)

    # FIXME: remove when references to DML_ZONE and DML_REGION are eliminated
    if globals().get('DML_API_ENDPOINT') is not None:
        url = urlparse(globals()['DML_API_ENDPOINT'])
        m = re.search(r'^api\.([^-]+)-([^.]+)\.', url.netloc)
        if m is not None and len(m.groups()) > 1:
            set_global('DML_ZONE', m.group(1))
            set_global('DML_REGION', m.group(2))

    def get_config_dir(_global):
        config_dir = USER_HOME_DIR if _global else os.getcwd()
        return os.path.join(config_dir, '.dml')

    def get_config_file(name, _global):
        return os.path.join(get_config_dir(_global), name)

    def read_config(name, _global):
        path = get_config_file(name, _global)
        config = ConfigParser(interpolation=ExtendedInterpolation())
        if os.path.exists(path):
            config.read(path)
        return config

    def set_config(config, profile, k, v):
        if profile not in config:
            config[profile] = {}
        config[profile][k] = v

    def write_config(config, name, _global):
        Path(get_config_dir(_global)).mkdir(mode=0o700, parents=True, exist_ok=True)
        config_file = get_config_file(name, _global)
        print(f'Writing config file: {config_file}', file=sys.stderr)
        with open(config_file, 'w') as f:
            config.write(f)

    def update_config(profile, group_id, api_endpoint, _global=False):
        config = read_config('config', _global)
        if group_id is not None:
            set_config(config, profile, 'group_id', group_id)
        if api_endpoint is not None:
            set_config(config, profile, 'api_endpoint', api_endpoint)
        write_config(config, 'config', _global)

    def update_credentials(profile, api_key):
        config = read_config('credentials', True)
        set_config(config, profile, 'api_key', api_key)
        write_config(config, 'credentials', True)

    return update_config, update_credentials


update_config, update_credentials = configure()

if LOCALSTACK_HOST is not None:
    AWS_LOCALSTACK_ENDPOINT = 'http://{}:{}'.format(LOCALSTACK_HOST, EDGE_PORT)
