import requests


class BaseSmokeTest:
    def __init__(self):
        print(f'===> Start {self.__class__.__name__}')


def test_endpoint(endpoint, method: str, headers=None, params=None, json=None, data=None):
    res = requests.request(
        method=method,
        url=endpoint,
        headers=headers,
        params=params,
        json=json,
        data=data
    )
    print(f'{res.status_code} - {method} {endpoint}')
    return res