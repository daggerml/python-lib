import configparser
import os
import pathlib

USER_HOME_DIR = str(pathlib.Path.home())

LOCALSTACK_HOST = os.getenv('LOCALSTACK_HOST')
EDGE_PORT = os.getenv('EDGE_PORT', '4566')
AWS_LOCALSTACK_ENDPOINT = None

DML_ZONE = os.getenv('DML_ZONE')
DML_REGION = os.getenv('DML_REGION')
DML_API_ENDPOINT = os.getenv('DML_API_ENDPOINT')
DML_API_KEY = os.getenv('DML_API_KEY')
DML_PROFILE = os.getenv('DML_PROFILE')


def configure():
    keys = {
        'config': {
            'zone': 'DML_ZONE',
            'region': 'DML_REGION',
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

    default_config_values = {
        'DML_ZONE': 'prod',
        'DML_REGION': 'us-west-2',
    }

    def from_file(config_dir, file_type, profile):
        config_file = os.path.join(config_dir, file_type)
        if os.path.exists(config_file) and profile is not None:
            config = configparser.ConfigParser()
            config.read(config_file)
            if profile in config:
                section = config[profile]
                for (k, v) in section.items():
                    ks = keys[file_type]
                    if k in ks:
                        var = ks[k]
                        if globals()[var] is None:
                            globals()[var] = v
                    else:
                        loc = config_file + ': in [' + str(section.name) + ']'
                        raise RuntimeError('invalid key: ' + k + ': in ' + loc)

    for d in config_dirs:
        for f in config_files:
            for p in profiles:
                from_file(os.path.join(d, '.dml'), f, p)

    for (k, v) in default_config_values.items():
        if globals()[k] is None:
            globals()[k] = v


configure()

if DML_API_ENDPOINT is None:
    DML_API_ENDPOINT = 'https://api.{}-{}.daggerml.com'.format(DML_ZONE, DML_REGION)

if LOCALSTACK_HOST is not None:
    AWS_LOCALSTACK_ENDPOINT = 'http://{}:{}'.format(LOCALSTACK_HOST, EDGE_PORT)
