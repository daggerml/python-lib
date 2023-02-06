import daggerml as dml
import daggerml._config as config
import daggerml._clink as clink
from getpass import getpass


@clink.arg('--profile', default='DEFAULT', help='configuration profile')
@clink.arg('--version', action='version', version=dml.__version__)
@clink.cli(description='DaggerML command line tool')
def cli(*args, **kwargs):
    raise Exception('no command specified')


@clink.arg('--global', action='store_true', dest='_global', help='update global configuration')
@clink.arg('--api-endpoint', help='API endpoint')
@cli.command(help='configure DaggerML API')
def configure(profile, api_endpoint, _global):
    if api_endpoint:
        config.update_config(profile, api_endpoint, _global)


@clink.arg('--dag-name', help='name of DAG to list')
@cli.command(help='list DAGs')
def list_dags(dag_name, **kwargs):
    return dml.list_dags(dag_name)


@clink.arg('--dag-id', required=True, help='ID of DAG to describe')
@cli.command(help='describe DAGs')
def describe_dags(dag_id, **kwargs):
    print('asdfasdfasdfasd')
    return dml.describe_dag(dag_id)
