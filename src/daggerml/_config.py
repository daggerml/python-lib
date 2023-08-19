import logging
import os
import sys
from collections.abc import MutableMapping as Map
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path

logger = logging.getLogger(__name__)
USER_HOME_DIR = str(Path.home())

DML_TEST_LOCAL = os.getenv('DML_TEST_LOCAL')
DML_PROFILE = os.getenv('DML_PROFILE')
DML_API_ENDPOINT = os.getenv('DML_API_ENDPOINT')
DML_S3_BUCKET = os.getenv('DML_S3_BUCKET')
DML_S3_PREFIX = os.getenv('DML_S3_PREFIX', '')
DML_S3_ENDPOINT = None

if DML_TEST_LOCAL:
    DML_PROFILE = None
    DML_API_ENDPOINT = 'http://localhost:8080'
    DML_S3_ENDPOINT = 'http://localhost:4566'
    os.environ['AWS_ACCESS_KEY_ID'] = 'bogus'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'bogus'
    os.environ['AWS_REGION'] = 'bogus'
    os.environ.pop('AWS_PROFILE', None)
    logger.warning('Local test mode:')
    logger.warning(f'DML_API_ENDPOINT={DML_API_ENDPOINT}')
    logger.warning(f'DML_S3_ENDPOINT={DML_S3_ENDPOINT}')


def deep_merge(d, v):
    for key in v:
        if key in d and isinstance(d[key], Map) and isinstance(v[key], Map):
            deep_merge(d[key], v[key])
        else:
            d[key] = v[key]


def configure():
    keys = {
        'config': {
            'api_endpoint': 'DML_API_ENDPOINT',
            's3_bucket': 'DML_S3_BUCKET',
            's3_prefix': 'DML_S3_PREFIX',
        },
    }

    config_dirs = [
        os.getcwd(),
        USER_HOME_DIR,
    ]

    config_files = [
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

    def update_config(profile, api_endpoint, _global=False):
        config = read_config('config', _global)
        if api_endpoint is not None:
            set_config(config, profile, 'api_endpoint', api_endpoint)
        write_config(config, 'config', _global)

    return update_config


update_config = configure()
