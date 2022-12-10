import boto3
import click
import configparser
import logging
import os


logger = logging.getLogger(__name__)
region = 'us-west-2'
user_pool = 'us-west-2_2jv1wrkxu'  # 'us-west-2_QDBiyDDQe'
CF_VALID_STATES = ['CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE',
                   'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_COMPLETE',
                   'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS']
OPS = ['login']


def get_user_pool_id(zone):
    client = boto3.client('cloudformation')
    out = []
    for x in client.get_paginator('list_stacks').paginate(StackStatusFilter=CF_VALID_STATES):
        out += [y for y in x['StackSummaries'] if y['StackName'].startswith(f'{zone}-core-Cognito')]
    if len(out) > 1:
        raise RuntimeError('ambiguous stack names. Please specify user-pool-id.')
    if len(out) == 0:
        raise RuntimeError('Bad zone. Please correct zone or specify user-pool-id.')
    stack_name = out[0]['StackName']
    resp = client.describe_stack_resource(StackName=stack_name,
                                          LogicalResourceId='UserPool')
    return resp['StackResourceDetail']['PhysicalResourceId']


def get_user_pool_client_id(user_pool_id):
    client = boto3.client('cognito-idp')
    out = []
    for x in client.get_paginator('list_user_pool_clients').paginate(UserPoolId=user_pool_id):
        out += x['UserPoolClients']
    if len(out) > 1:
        raise RuntimeError('ambiguous user pool clients. Please specify user-pool-client.')
    if len(out) == 0:
        raise RuntimeError('Please correct user-pool-id or specify user-pool-client.')
    return out[0]['ClientId']


def get_user_pool_and_client(zone, user_pool_id, user_pool_client_id):
    if user_pool_id is None:
        if zone is None:
            raise ValueError('Please specify zone or user_pool_id')
        user_pool_id = get_user_pool_id(zone)
    if user_pool_client_id is None:
        user_pool_client_id = get_user_pool_client_id(user_pool_id)
    return user_pool_id, user_pool_client_id


def login(zone, user_pool_id, user_pool_client_id, profile, is_local, password, username=None):
    base_dir = os.path.abspath('.') if is_local else os.path.expanduser('~')
    base_dir = os.path.join(base_dir, '.dml')
    if not os.path.isdir(base_dir):
        os.mkdir(base_dir)
    creds_file = os.path.join(base_dir, 'credentials')
    user_pool_id, user_pool_client_id = get_user_pool_and_client(
        zone, user_pool_id, user_pool_client_id
    )
    client = boto3.client('cognito-idp')
    creds = configparser.ConfigParser()
    if os.path.isfile(creds_file):
        creds.read(creds_file)
    if username is None:
        if profile not in creds:
            raise RuntimeError('no username in profile')
        username = creds[profile].get('username')
        if username is None:
            raise RuntimeError('no username in profile')
    if password is None:
        password = input('please input password: ')
    logger.info('authenticating %s...', username)
    resp = client.initiate_auth(
        AuthFlow='USER_PASSWORD_AUTH',
        AuthParameters={'USERNAME': username, 'PASSWORD': password},
        ClientId=user_pool_client_id,
    )
    challenge = resp.get('ChallengeName')
    session = resp.get('Session')

    if challenge == 'NEW_PASSWORD_REQUIRED':
        logger.info('Forced password change for user: %s...', username)
        password = input('enter new password: ')
        resp = client.respond_to_auth_challenge(
            ClientId=user_pool_client_id,
            ChallengeName=challenge,
            ChallengeResponses={'NEW_PASSWORD': password, 'USERNAME': username},
            Session=session
        )

    logger.info('user: %s authenticated successfully...', username)
    print('user: %s authenticated successfully...' % username)
    creds[profile] = {
        'api_key': resp['AuthenticationResult']['RefreshToken'],
        'username': username
    }
    with open(creds_file, 'w') as f:
        creds.write(f)
    return True


@click.group()
def cli():
    pass


@cli.command('login', context_settings={'auto_envvar_prefix': 'DML', 'show_default': True})
@click.option('-z', '--zone', help='the cfn zone')
@click.option('-u', '--username', help='will try to read from creds file if not specified.')
@click.option('-p', '--profile', default='DEFAULT', help='profile to use')
@click.option('-l', '--local', is_flag=True, help='write config to global or local?')
@click.option('--user-pool-id', help='the user pool id')
@click.option('--user-pool-client-id', help='pool client id')
@click.password_option(help='feed password into stdin?')
def cli_login(zone, user_pool_id, user_pool_client_id, profile, local, username, password):
    return login(zone, user_pool_id, user_pool_client_id, profile, local, password, username)
