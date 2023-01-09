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
@clink.arg('--group-id', help='group ID')
@clink.arg('--api-endpoint', help='API endpoint')
@cli.command(help='configure DaggerML API')
def configure(profile, group_id, api_endpoint, _global):
    if group_id or api_endpoint:
        config.update_config(profile, group_id, api_endpoint, _global)


@clink.arg('--username', required=True, help='user name')
@cli.command(help='create DaggerML API key')
def login(profile, username):
    password = getpass()
    resp = dml.login(username, password)
    config.update_credentials(profile, resp['api_key'])


@clink.arg('--dag-name', help='name of DAG to list')
@cli.command(help='list DAGs')
def list_dags(dag_name, **kwargs):
    return dml.list_dags(dag_name)


@clink.arg('--dag-id', required=True, help='ID of DAG to describe')
@cli.command(help='describe DAGs')
def describe_dags(dag_id, **kwargs):
    return dml.describe_dag(dag_id)
