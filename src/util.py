import requests
import json

session = requests.Session()


def login(csrf: str, base: str, username: str, password: str) -> str:
    resp: requests.Response = session.post(base + '/sign_in/', data={
        'data': json.dumps({'p_username': username, 'p_pwd': password})
    }, headers={
        'cookie': f'omnidb_csrftoken={csrf}',
        'x-csrftoken': csrf,
        'x-requested-with': 'XMLHttpRequest'
    })
    if resp.status_code != 200:
        raise ValueError(
            f"Login failed, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")

    # omnidb_sessionid
    setcookie = resp.headers['Set-Cookie']
    splited = setcookie.split(';')
    for i in range(len(splited)):
        kv = splited[i].strip().split('=')
        if kv[0] == 'omnidb_sessionid':
            sessionid = kv[1]
            break

    print(f"omnidb_sessionid={sessionid}")
