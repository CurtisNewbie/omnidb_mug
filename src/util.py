import requests
import json
from websocket import create_connection, WebSocket

DEFAULT_PROTOCOL = "https://"
session = requests.Session() # reuse connection

class OSession:
    def __init__(self, set_cookie: str, csrf_token: str, host: str, protocol: str):
        self.sessionid = ""
        self.host = host 
        self.protocol = protocol
        self.csrf = csrf_token

        for v in set_cookie.split(';'):
            kv = v.strip().split('=')
            if kv[0] == 'omnidb_sessionid':
                self.sessionid = kv[1]
                break

        self.cookie = f"omnidb_sessionid={self.sessionid}; omnidb_csrftoken={csrf_token}"


def login(csrf: str, host: str, username: str, password: str, protocol: str = DEFAULT_PROTOCOL) -> "OSession":
    url = protocol + host + '/sign_in/'
    print(f"Trying to login, url: {url}")

    resp: requests.Response = session.post(url, data={
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
    if not 'Set-Cookie' in resp.headers:
        raise ValueError(
            f"Login failed, unable to find set-cookie, make sure that you are not already signed-in in your browser, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")

    sh = OSession(resp.headers['Set-Cookie'], csrf, host, protocol)
    print(f"omnidb_sessionid={sh.sessionid}")
    if not sh.sessionid:
        raise ValueError(
            f"Login failed, unable to find omnidb_sessionid, make sure that you are not already signed-in in your browser, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")

    return sh


def send_recv(ws: WebSocket, payload, log_msg=True, wait_recv_times=1) -> list[str]:
    ws.send(payload)
    if log_msg: print(f"ws sent: '{payload}'")
    r = []
    for i in range(wait_recv_times):
        rcv = ws.recv()
        r.append(rcv)
        if log_msg: print(f"ws received: [{i}] '{rcv}'")
    return r


def exec_query(ws: WebSocket, sql: str, **kw) -> tuple[list[str],list[list[str]]]:
    msg = f'{{"v_code":1,"v_context_code":{kw["v_context_code"]},"v_error":false,"v_data":{{"v_sql_cmd":"{sql}","v_sql_save":"{sql}","v_cmd_type":null,"v_db_index":{kw["v_db_index"]},"v_conn_tab_id":"{kw["v_conn_tab_id"]}","v_tab_id":"{kw["v_tab_id"]}","v_tab_db_id":{kw["v_tab_db_id"]},"v_mode":0,"v_all_data":false,"v_log_query":true,"v_tab_title":"Query","v_autocommit":true}}}}'
    resp = send_recv(ws, msg, False, 2)
    j: dict = json.loads(resp[1])
    col = j["v_data"]["v_col_names"]
    rows = j["v_data"]["v_data"]
    print(f"Columns: {col}")
    for i in range(len(rows)):
        print(f"Row[{i}]: {rows[i]}")
    print()
    return [col, rows]


def connect_ws(sh: OSession, host: str, protocol: str = "wss://") -> WebSocket:
    url = protocol + host
    if not url.endswith("/"):
        url += "/"
    url += "wss"
    print(f"Connecting to websocket server, url: {url}")
    ws = create_connection(
        url, headers=["Upgrade: websocket"], cookie=sh.cookie)
    print(f"Successfully connected to websocket server")
    return ws


def change_active_database(sh: OSession, p_database_index, p_tab_id, p_database):
    url = sh.protocol + sh.host + '/change_active_database/'
    print(f"Changing active database, url: '{url}'")

    resp: requests.Response = session.post(url, data={
        'data': json.dumps({'p_database_index': p_database_index, 'p_tab_id': p_tab_id, "p_database": p_database})
    }, headers={
        'cookie': sh.cookie,
        'x-csrftoken': sh.csrf,
        'x-requested-with': 'XMLHttpRequest'
    })
    if resp.status_code != 200:
        raise ValueError(
            f"Change active database failed, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")
