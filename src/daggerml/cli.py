import click
import sys
import daggerml as dml
import daggerml._config as config
import logging
from daggerml._config import DML_PROFILE, DML_GROUP_ID, DML_API_ENDPOINT


logger = logging.getLogger(__name__)


@click.group(context_settings={'show_default': True,
                               'help_option_names': ['-h', '--help'],
                               'auto_envvar_prefix': 'DML'})
def cli():
    pass


@cli.command('configure', context_settings={'auto_envvar_prefix': 'DML'},
             help='configure DaggerML API')
@click.option('--global/--local', '_global', help='update global configuration')
@click.option('--profile', default=DML_PROFILE or 'DEFAULT', help='configuration profile')
@click.option('--group-id', default=DML_GROUP_ID, help='group ID')
@click.option('--api-endpoint', default=DML_API_ENDPOINT, help='API endpoint')
def cli_configure(profile, group_id, api_endpoint, _global):
    config.update_config(profile, group_id, api_endpoint, _global)


@cli.command('login', context_settings={'auto_envvar_prefix': 'DML'},
             help='create DaggerML API key')
@click.option('--profile', help='configuration profile')
@click.option('--username', help='user name')
@click.option('--password', required=True, prompt=True, hide_input=True,
              help='read from stdin if not specified')
def cli_login(profile, username, password):
    resp = dml.login(username, password)
    config.update_credentials(profile, resp['api_key'])


@cli.group(context_settings={'auto_envvar_prefix': 'DML_DAG'},
           help='create DaggerML API key')
def dag():
    pass


@dag.command('list', help='list dags')
@click.option('-n', '--name',
              help='optional name to filter on')
def dag_list(name):
    click.echo(dml.list_dags(name))


@dag.command('describe', help='list dags')
@click.argument('dag_id', help='dag ID')
def dag_describe(dag_id):
    click.echo(dml.describe_dag(dag_id))


if __name__ == '__main__':
    cli(sys.argv[1:])
