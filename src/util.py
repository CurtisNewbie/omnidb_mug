import sys
import requests
import json
import re
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
    if not sh.sessionid:
        raise ValueError(
            f"Login failed, unable to find omnidb_sessionid, make sure that you are not already signed-in in your browser, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")

    print(f"cookie: {sh.cookie}")
    return sh


def ws_send_recv(ws: WebSocket, payload, log_msg=True, wait_recv_times=1) -> list[str]:
    ws.send(payload)
    if log_msg: print(f"ws sent: '{payload}'")
    r = []
    for i in range(wait_recv_times):
        rcv = ws.recv()
        r.append(rcv)
        if log_msg: print(f"ws received: [{i}] '{rcv}'")
    return r


def sjoin(cnt: int, token: str) -> str:
    s = ""
    for i in range(cnt): s += token
    return s


def spaces(cnt: int) -> str:
    return sjoin(cnt, " ")


def query_has_limit(sql: str) -> bool:
    return re.match(".* ?[Ll][Ii][Mm][Ii][Tt]", sql)


def exec_query(ws: WebSocket, sql: str, **kw) -> tuple[list[str],list[list[str]]]:
    msg = f'{{"v_code":1,"v_context_code":{kw["v_context_code"]},"v_error":false,"v_data":{{"v_sql_cmd":"{sql}","v_sql_save":"{sql}","v_cmd_type":null,"v_db_index":{kw["v_db_index"]},"v_conn_tab_id":"{kw["v_conn_tab_id"]}","v_tab_id":"{kw["v_tab_id"]}","v_tab_db_id":{kw["v_tab_db_id"]},"v_mode":0,"v_all_data":false,"v_log_query":true,"v_tab_title":"Query","v_autocommit":true}}}}'
    resp = ws_send_recv(ws, msg, kw["log_msg"], 2)
    j: dict = json.loads(resp[1])
    if j["v_error"]:
        errmsg = j["v_data"]["message"]
        print(f"Error: '{errmsg}'")
        return [[], []]

    col = j["v_data"]["v_col_names"]
    rows = j["v_data"]["v_data"]
    cost = j["v_data"]["v_duration"]
     
    indent : dict[int][int] = {}
    for i in range(len(col)): indent[i] = len(col[i])
    for r in rows: 
        for i in range(len(col)):
            cl = len(r[i])
            if cl > indent[i]: indent[i] = cl # max length among the rows

    col_title = "| "
    col_sep = "|-"
    for i in range(len(col)): 
        col_title += col[i] + spaces(indent[i] - len(col[i]) + 1) + " | "
        col_sep += sjoin(indent[i] + 1, "-") + "-|"
        if i < len(col) - 1: col_sep += "-"
    print(col_sep + "\n" + col_title + "\n" + col_sep)

    for r in rows:
        row_ctn = "| "
        for i in range(len(col)): row_ctn += r[i] + spaces(1 + indent[i] - len(r[i])) + " | "
        print(row_ctn)
    print(col_sep)

    print()
    print(f"Total: {len(rows)}")
    print(f"Cost : {cost}")
    print()
    return [col, rows]


def ws_connect(sh: OSession, host: str, protocol: str = "wss://") -> WebSocket:
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
