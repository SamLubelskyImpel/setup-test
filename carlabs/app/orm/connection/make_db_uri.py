from typing import Literal
# TODO refactor this according to this project needs
# import os

# from spincar_lib.get_secrets import get_secret, get_secret_using_role
# from spincar_lib.regions import REGION

# secret = get_secret('integrations/db', no_cache=True)

# DB_HOST = {
#     'us': secret['DB_HOST_US'],
#     'eu': secret['DB_HOST_EU']
# }
# DB_USERNAME = {
#     'us': secret['DB_USERNAME_US'],
#     'eu': secret['DB_USERNAME_EU']
# }
# DB_PASSWORD = {
#     'us': secret['DB_PASSWORD_US'],
#     'eu': secret['DB_PASSWORD_EU']
# }

# DB_NAME = {
#     'dev': {
#         'us': 'integrations-dev',
#         'eu': 'integrations-dev-eu'
#     },
#     'prod': {
#         'us': 'integrations-prod',
#         'eu': 'integrations-prod-eu'
#     },
#     'test': {
#         'us': 'integrations-dev',
#         'eu': 'integrations-dev-eu'
#     }
# }[FLAVOR]


DB = {
    'CARLABS': {
        'DB_USERNAME': 'integrator',
        'DB_PASSWORD': 'CarLabs2022!',
        'DB_HOST': 'postgres.proxy.carlabs.com:45432',
        'DB_NAME': 'data_integrations'
    },
    'SHARED_DMS': {
        'DB_USERNAME': 'postgres',
        'DB_PASSWORD': 'root',
        'DB_HOST': 'localhost:54321',
        'DB_NAME': 'dms'
    }
    # 'SHARED_DMS': {
    #     'DB_USERNAME': 'developer',
    #     'DB_PASSWORD': '31eeeb5d8805f3f6e0ee686a47f47bd5',
    #     'DB_HOST': 'unified-data-test-1.c8eqxn0581ih.us-east-1.rds.amazonaws.com:5432',
    #     'DB_NAME': 'dms'
    # }
}


def make_db_uri(db: Literal['CARLABS', 'SHARED_DMS'], region=None):
    app_name = 'carlabs-integration'
    return 'postgresql://{}:{}@{}/{}?application_name={}'.format(
        DB[db]['DB_USERNAME'],
        DB[db]['DB_PASSWORD'],
        DB[db]['DB_HOST'],
        DB[db]['DB_NAME'],
        app_name)
