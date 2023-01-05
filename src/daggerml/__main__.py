import click
import sys
import daggerml as dml
import daggerml._config as config
import logging


logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command('configure', context_settings={'show_default': True}, help='configure DaggerML API')
@click.option('--global', is_flag=True, help='update global configuration')
@click.option('--profile', default='DEFAULT', help='configuration profile')
@click.option('--group-id', default=None, help='group ID')
@click.option('--api-endpoint', default=None, help='API endpoint')
def cli_configure(profile, group_id, api_endpoint, **kwargs):
    config.update_config(profile, group_id, api_endpoint, kwargs['global'])


@cli.command('login', context_settings={'show_default': True}, help='create DaggerML API key')
@click.option('--profile', default='DEFAULT', help='configuration profile')
@click.option('--username', required=True, help='user name')
@click.option('--password', required=True, prompt=True, hide_input=True, help='read from stdin if not specified')
def cli_login(profile, username, password):
    resp = dml.login(username, password)
    config.update_credentials(profile, resp['api_key'])


if __name__ == '__main__':
    cli(sys.argv[1:])
