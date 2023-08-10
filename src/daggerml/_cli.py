import daggerml as dml
import daggerml._clink as clink
import daggerml._config as config


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
@cli.command(help='describe a DAG')
def describe_dag(dag_id, **kwargs):
    return dml.describe_dag(dag_id)


@clink.arg('--dag-id', required=True, help='ID of DAG to delete')
@cli.command(help='delete a DAG')
def delete_dag(dag_id, **kw):
    return dml.delete_dag(dag_id)


@clink.arg('--node-id', required=True, help='ID of Node to get')
@clink.arg('--secret', required=True, help='The secret of the dag')
@cli.command(help='describe DAGs')
def get_node(node_id, secret, **kw):
    return dml.get_node(node_id=node_id, secret=secret)


@clink.arg('--node-id', required=True, help='ID of Node to get')
@clink.arg('--secret', required=True, help='The secret of the dag')
@cli.command(help='describe DAGs')
def get_node_metadata(node_id, secret, **kw):
    return dml.get_node_metadata(node_id=node_id, secret=secret)
