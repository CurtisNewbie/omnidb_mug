import sys
import unicodedata
import requests
import json
import re
from websocket import create_connection, WebSocket

DEFAULT_PROTOCOL = "https://"
session = requests.Session() # reuse connection

class OTab:
    def __init__(self, index, tab_db_id, title):
        self.index = index 
        self.tab_db_id = tab_db_id
        self.title = title 

class OConnection:
    def __init__(self, v_alias, v_conn_id):
        self.v_alias = v_alias
        self.v_conn_id = v_conn_id

class ODatabase:
    def __init__(self, connections: list[OConnection], tabs: list[OTab]):
        self.connections = connections
        self.tabs = tabs

class OSession:
    def __init__(self, set_cookie: str, csrf_token: str, host: str, protocol: str):
        self.sessionid = ""
        self.host = host 
        self.protocol = protocol
        self.csrf = csrf_token

        self.sessionid = parse_set_cookie(set_cookie, 'omnidb_sessionid')
        self.cookie = f"omnidb_sessionid={self.sessionid}; omnidb_csrftoken={csrf_token}"


def get_csrf_token(host: str, protocol: str = DEFAULT_PROTOCOL, debug = False) -> str:
    url = protocol + host + "/"
    if debug: print(f"Trying to get csrf token, url: {url}")
    resp: requests.Response = session.get(url)
    if not 'Set-Cookie' in resp.headers: sys_exit(1, f"Failed to retrieve csrf token")
    csrf = parse_set_cookie(resp.headers['Set-Cookie'], 'omnidb_csrftoken')
    if debug: print(f"csrf token: '{csrf}'")
    return csrf


def parse_set_cookie(header: str, key: str) -> str:
    for v in header.split(';'):
        kv = v.strip().split('=')
        if kv[0] == key:
            return kv[1]
    return None


def login(csrf: str, host: str, username: str, password: str, protocol: str = DEFAULT_PROTOCOL, debug = False) -> "OSession":
    url = protocol + host + '/sign_in/'
    if debug: print(f"Trying to login, url: {url}")

    resp: requests.Response = session.post(url, data={
        'data': json.dumps({'p_username': username, 'p_pwd': password})
    }, headers={
        'cookie': f'omnidb_csrftoken={csrf}',
        'x-csrftoken': csrf,
        'x-requested-with': 'XMLHttpRequest'
    })

    if resp.status_code != 200:
        sys_exit(1, f"Login failed, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")
    if not 'Set-Cookie' in resp.headers:
        sys_exit(1, f"Login failed, password incorrect")

    # parse cookie 
    sh = OSession(resp.headers['Set-Cookie'], csrf, host, protocol)
    if not sh.sessionid:
        sys_exit(1, f"Login failed, unable to extract cookie in response, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")

    if debug: print(f"cookie: {sh.cookie}")
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


def sys_exit(status: int, msg: str):
    if msg: print(msg)
    sys.exit(status)


def sjoin(cnt: int, token: str) -> str:
    s = ""
    for i in range(cnt): s += token
    return s


def spaces(cnt: int) -> str:
    return sjoin(cnt, " ")


def query_has_limit(sql: str) -> bool:
    return re.match(".* ?[Ll][Ii][Mm][Ii][Tt]", sql)


def str_width(s: str) -> int:
    l = 0
    for i in range(len(s)): 
        w = unicodedata.east_asian_width(s[i])
        l += 2 if w in ['W', 'F', 'A'] else 1
    return l


def exec_query(ws: WebSocket, sql: str, **kw) -> tuple[bool, list[str],list[list[str]]]:
    msg = f'{{"v_code":1,"v_context_code":{kw["v_context_code"]},"v_error":false,"v_data":{{"v_sql_cmd":"{sql}","v_sql_save":"{sql}","v_cmd_type":null,"v_db_index":{kw["v_db_index"]},"v_conn_tab_id":"{kw["v_conn_tab_id"]}","v_tab_id":"{kw["v_tab_id"]}","v_tab_db_id":{kw["v_tab_db_id"]},"v_mode":0,"v_all_data":false,"v_log_query":true,"v_tab_title":"Query","v_autocommit":true}}}}'
    resp = ws_send_recv(ws, msg, kw["log_msg"], 2)
    j: dict = json.loads(resp[1])
    if j["v_error"]:
        errmsg = j["v_data"]["message"]
        print(f"Error: '{errmsg}'")
        return [False, [], []]
    
    if not "v_data" in j:
        print("Unable to find 'v_data' in response, connection may have lost")
        return [False, [], []]

    col = j["v_data"]["v_col_names"]
    rows = j["v_data"]["v_data"]
    cost = j["v_data"]["v_duration"]
     
    # max length among the rows
    indent : dict[int][int] = {}
    for i in range(len(col)): indent[i] = str_width(col[i])
    for r in rows: 
        for i in range(len(col)): indent[i] = max(indent[i], len(r[i]))

    if len(col) > 0:
        print()
        col_title = "| "
        col_sep = "|-"
        for i in range(len(col)): 
            col_title += col[i] + spaces(indent[i] - str_width(col[i]) + 1) + " | "
            col_sep += sjoin(indent[i] + 1, "-") + "-|"
            if i < len(col) - 1: col_sep += "-"
        print(col_sep + "\n" + col_title + "\n" + col_sep)

        for r in rows:
            row_ctn = "| "
            for i in range(len(col)): row_ctn += r[i] + spaces(1 + indent[i] - str_width(r[i])) + " | "
            print(row_ctn)
        print(col_sep)
    print()
    print(f"Total: {len(rows)}")
    print(f"Cost : {cost}")
    print()
    return [True, col, rows]


def ws_connect(sh: OSession, host: str, protocol: str = "wss://", debug=False) -> WebSocket:
    url = protocol + host
    if not url.endswith("/"):
        url += "/"
    url += "wss"
    if debug: print(f"Connecting to websocket server, url: {url}")
    ws = create_connection(
        url, headers=["Upgrade: websocket"], cookie=sh.cookie)
    if debug: print(f"Successfully connected to websocket server")
    return ws


def change_active_database(sh: OSession, p_database_index, p_tab_id, p_database, debug = False):
    url = sh.protocol + sh.host + '/change_active_database/'
    if debug: print(f"Changing active database, url: '{url}'")

    resp: requests.Response = session.post(url, data={
        'data': json.dumps({'p_database_index': p_database_index, 'p_tab_id': p_tab_id, "p_database": p_database})
    }, headers={
        'cookie': sh.cookie,
        'x-csrftoken': sh.csrf,
        'x-requested-with': 'XMLHttpRequest'
    })
    if resp.status_code != 200:
        sys_exit(1, f"Change active database failed, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")


def get_database_list(sh: OSession, debug = False) -> ODatabase:
    url = sh.protocol + sh.host + '/get_database_list/'
    if debug: print(f"Get database list, url: '{url}'")

    resp: requests.Response = session.post(url, data={'data': ''}, headers={
        'cookie': sh.cookie,
        'x-csrftoken': sh.csrf,
        'x-requested-with': 'XMLHttpRequest'
    })
    if resp.status_code != 200:
        sys_exit(1, f"Get database list failed, code: {resp.status_code}, msg: {resp.text}, headers: {resp.headers}")
    
    j = json.loads(resp.text)
    connections = []
    if 'v_connections' in j['v_data']:
        for t in j['v_data']['v_connections']:
            connections.append(OConnection(t['v_alias'], t['v_conn_id']))
    
    tabs = []
    if 'v_existing_tabs' in j['v_data']:
        for t in j['v_data']['v_existing_tabs']:
            tabs.append(OTab(t['index'], t['tab_db_id'], t['title']))

    return ODatabase(connections, tabs)  


