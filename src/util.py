import io
import sys
import time
import unicodedata
import requests
import json
import re
import datetime
from websocket import create_connection, WebSocket

DEFAULT_HTTP_PROTOCOL = "https://"
DEFAULT_WS_PROTOCOL = "wss://"
session = requests.Session() # reuse connection

TP_SELECT = 0
TP_SHOW_TABLE = 1
TP_SHOW_CREATE_TABLE = 2
TP_DESC = 3
TP_USE_DB = 4
TP_OTHER = -1

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


def quote_list(l: list[str]):
    j = []
    for i in range(len(l)): j.append(f"'{l[i]}'")
    return j


def extract_schema_table(sql: str) -> tuple[str]:
    res = re.search(r"^select .* from ([^ \.]+).([^ \.;]+).*$", sql, re.IGNORECASE)
    return res.group(1), res.group(2)


def filter_by_idx(l: list[str], idx: set[int]) -> list[str]:
    r = []
    for i in range(len(l)):
        if i in idx: continue
        r.append(l[i])
    return r


def collect_filter_idx(l: list[str], excl: set[str]) -> set[int]:
    idx = set()
    if not excl: return idx
    for i in range(len(l)):
        if l[i] in excl: idx.add(i)
    return idx


def dump_insert_sql(sql: str, cols: list[str], rows: list[list[str]], excl: set[str] = None):
    schema, table = extract_schema_table(sql)

    filter_idx =  collect_filter_idx(cols, excl)
    joined_cols = ",".join(filter_by_idx(cols, filter_idx))

    qrows = []
    for r in rows:
        fr = filter_by_idx(r, filter_idx)
        qrows.append("(" + ",".join(quote_list(fr)) + ")")

    joined_rows = ",\n\t".join(qrows)
    sql = f"INSERT INTO {schema}.{table} ({joined_cols}) VALUES \n\t{joined_rows};"
    print(sql)


def guess_qry_type(sql: str) -> int:
    if re.match(r"^select .* from.*", sql, re.IGNORECASE): return TP_SELECT
    if re.match(r"^show +tables.*", sql, re.IGNORECASE): return TP_SHOW_TABLE
    if re.match(r"^desc .*", sql, re.IGNORECASE): return TP_DESC
    if re.match(r"^show +create +table.*", sql, re.IGNORECASE): return TP_SHOW_CREATE_TABLE
    if re.match(r"^use .*", sql, re.IGNORECASE): return TP_USE_DB
    return TP_OTHER

_slt_sql_pat = re.compile(r"^select .* from +(\.?[\`\w_]+)(?: *| +.*);?$", re.IGNORECASE)
_show_tb_pat = re.compile(r"^show +tables( *)(?:| +like [^ ]+) *;? *$", re.IGNORECASE)
_show_crt_tb_pat = re.compile(r"^show +create +table +([`0-9a-zA-Z_]+) *;? *$", re.IGNORECASE)
_desc_tb_pat = re.compile(r"^desc +(\.?[`0-9a-zA-Z_]+) *;? *$", re.IGNORECASE)
def auto_complete_db(sql: str, database: str) -> str:
    '''
    TODO: deprecated, it's not very useful, since it only supports simple queries
    '''
    start = time.monotonic_ns()
    if not database: return sql

    sql = sql.strip()

    completed = False
    m = _slt_sql_pat.match(sql)
    if m:
        open, close = m.span(1)
        sql = insert_db_name(sql, database, open, close)
        completed = True

    if not completed:
        m = _show_tb_pat.match(sql)
        if m:
            open, close = m.span(1)
            sql = sql[: open] + f" in {database}" + sql[close:]
            completed = True

    if not completed:
        m = _show_crt_tb_pat.match(sql)
        if m:
            open, close = m.span(1)
            sql = insert_db_name(sql, database, open, close)
            completed = True

    if not completed:
        m = _desc_tb_pat.match(sql)
        if m:
            open, close = m.span(1)
            sql = insert_db_name(sql, database, open, close)
            completed = True

    print(f"Auto-completed ({(time.monotonic_ns() - start) / 1e6:.3f}ms): {sql}")
    return sql


_pretty_print_pat = re.compile(r"^.*(\\G) *;?$", re.IGNORECASE)
def parse_pretty_print(sql: str) -> tuple[bool, str]:
    m = _pretty_print_pat.match(sql)
    if not m: return False, sql

    open, close = m.span(1)
    sql = sql[: open] + "" + sql[close:] # remove the \G
    return True, sql.strip()


def insert_db_name(sql: str, database: str, open: int, close: int) -> str:
    table = sql[open:close].strip()
    l = table.find(".")
    if l < 0: table = "." + table
    table = database + table
    sql = sql[: open] + table + sql[close:]
    return sql


is_show_crt_tb_pat = re.compile(r"^show +create +table +[\.`0-9a-zA-Z_]+ *;? *$", re.IGNORECASE)
def is_show_create_table(sql: str) -> bool:
    return is_show_crt_tb_pat.match(sql)


is_select_pat = re.compile(r"^select.*$", re.IGNORECASE)
def is_select(cmd: str) -> bool:
    return is_select_pat.match(cmd)


exit_pat = re.compile(r"^(?:quit|exit|\\quit|\\exit)(?:|\(\))$", re.IGNORECASE)
def is_exit(cmd: str) -> bool:
    return exit_pat.match(cmd)


def env_print(key, value):
    prop = key + ":"
    print(f"{prop:40}{value}")


def get_csrf_token(host: str, protocol: str = DEFAULT_HTTP_PROTOCOL, debug = False) -> str:
    url = protocol + host + "/"
    if debug: print(f"[debug] trying to get csrf token, url: {url}")
    resp: requests.Response = session.get(url)
    if not 'Set-Cookie' in resp.headers: sys_exit(1, f"Failed to retrieve csrf token")
    csrf = parse_set_cookie(resp.headers['Set-Cookie'], 'omnidb_csrftoken')
    if debug: print(f"[debug] csrf token: '{csrf}'")
    return csrf


def close_ws(ws: WebSocket, debug = False):
    if ws: ws.close()
    if debug: print("[debug] websocket disconnected")


def parse_set_cookie(header: str, key: str) -> str:
    for v in header.split(';'):
        kv = v.strip().split('=')
        if kv[0] == key:
            return kv[1]
    return None


def login(csrf: str, host: str, username: str, password: str, protocol: str = DEFAULT_HTTP_PROTOCOL, debug = False) -> "OSession":
    url = protocol + host + '/sign_in/'
    if debug: print(f"[debug] Trying to login, url: {url}")

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

    if debug: print(f"[debug] cookie: {sh.cookie}")
    return sh


def ws_send_recv(ws: WebSocket, payload, debug=True, wait_recv_times=1) -> list[str]:
    ws.send(payload)
    if debug: print(f"[debug] ws sent: '{payload}'")
    r = []
    for i in range(wait_recv_times):
        rcv = ws.recv()
        r.append(rcv)
        if debug: print(f"[debug] ws received: [{i}] '{rcv}'")
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

class QueryContext:

    def __init__(self):
        self.debug = False
        self.v_db_index = ""
        self.v_context_code = 0
        self.v_conn_tab_id = ""
        self.v_tab_id = ""
        self.v_tab_db_id = ""
        self.logf : io.TextIOWrapper = None


escape_pat = re.compile(r'([^\\])(")')
def escape(sql: str) -> str:
    return re.sub(escape_pat, r'\1\\"', sql)


def exec_batch_query(ws: WebSocket, sql: str, qry_ctx: QueryContext, page_size: int = 400,
                     throttle_ms: int = 0, slient=False) -> tuple[bool, list[str], list[str]]:
    ok = True
    offset = 0
    acc_cols = []
    acc_rows = []
    if sql.endswith(";"): sql = sql[:len(sql) - 1].strip()

    while True:
        offset_sql = sql + f" limit {offset}, {page_size}"
        if not slient: print(offset_sql)

        ok, cols, rows = exec_query(ws=ws, sql=offset_sql, qc=qry_ctx, slient=slient, pretty=False)
        if not ok or len(rows) < 1: break # error or empty page

        if offset < 1: acc_cols = cols # first page
        acc_rows += rows # append rows
        offset += page_size # next page

        if len(rows) < page_size:  break # the end of pagination
        if throttle_ms > 0: time.sleep(throttle_ms / 1000) # throttle a bit, not so fast
    return ok, acc_cols, acc_rows


def exec_query(ws: WebSocket, sql: str, qc: QueryContext, slient = False, pretty = False) -> tuple[bool, list[str],list[list[str]]]:
    debug = qc.debug

    qc.v_context_code += 1
    if debug: print(f"[debug] QueryContext.v_context_code: {qc.v_context_code}")

    sql = escape(sql)
    if debug: print(f"[debug] executing query: '{sql}', slient: {slient}, pretty: {pretty}")
    if qc.logf: qc.logf.write(f"\n{datetime.datetime.now()} - {sql}")

    start = time.monotonic_ns()
    msg = f'{{"v_code":1,"v_context_code":{qc.v_context_code},"v_error":false,"v_data":{{"v_sql_cmd":"{sql}","v_sql_save":"{sql}","v_cmd_type":null,"v_db_index":{qc.v_db_index},"v_conn_tab_id":"{qc.v_conn_tab_id}","v_tab_id":"{qc.v_tab_id}","v_tab_db_id":{qc.v_tab_db_id},"v_mode":0,"v_all_data":false,"v_log_query":true,"v_tab_title":"Query","v_autocommit":true}}}}'

    resp = ws_send_recv(ws, msg, debug, 2)
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

    if not slient and len(col) > 0:
        if is_show_create_table(sql):
            print("\n" + rows[0][1])
        else:
            if pretty:

                # max length among the column names
                max_col_len = 0
                for c in col: max_col_len = max(str_width(c), max_col_len)
                if debug: print(f"[debug] max_col_len: {max_col_len}")

                sl = []
                for r in rows:
                    s = ""
                    for i in range(len(col)):
                        s += f"{spaces(max_col_len - str_width(col[i]))}{col[i]}: {r[i]}"
                        if i < len(col) - 1: s += "\n"
                    sl.append(s)

                print("\n******************************************\n")
                print("\n\n******************************************\n\n".join(sl))
                print("\n******************************************")
            else:
                # max length among the rows
                indent : dict[int][int] = {}
                for i in range(len(col)): indent[i] = str_width(col[i])
                for r in rows:
                    for i in range(len(col)): indent[i] = max(indent[i], str_width(r[i]))

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
        print(f"Total    : {len(rows)}")
        print(f"Cost     : {cost}")
        print(f"Wall Time: {(time.monotonic_ns() - start) / 1e6:.2f} ms")
        print()

    if debug: print(f"[debug] Total: {len(rows)}, Cost: {cost}, Wall Time: {(time.monotonic_ns() - start) / 1e6:.2f} ms")

    return [True, col, rows]


def ws_connect(sh: OSession, host: str, protocol: str = DEFAULT_WS_PROTOCOL, debug=False) -> WebSocket:
    url = protocol + host
    if not url.endswith("/"):
        url += "/"
    url += "wss"
    if debug: print(f"[debug] connecting to websocket server, url: {url}")
    ws = create_connection(
        url, headers=["Upgrade: websocket"], cookie=sh.cookie)
    if debug: print(f"[debug] successfully connected to websocket server")

    # first message
    ws_send_recv(ws, f'{{"v_code":0,"v_context_code":0,"v_error":false,"v_data":"{sh.sessionid}"}}', debug=debug)
    return ws


def change_active_database(sh: OSession, p_database_index, p_tab_id, p_database, debug = False):
    url = sh.protocol + sh.host + '/change_active_database/'
    if debug: print(f"[debug] changing active database, url: '{url}'")

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
    if debug: print(f"[debug] get database list, url: '{url}'")

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


