from pathlib import Path
import datetime
import argparse
import getpass
import os
import sys
import re
import readline # don't remove this, this is for input()
import time
import subprocess
import io
import unicodedata
import requests
import json
from websocket import create_connection, WebSocket
from os.path import abspath

# reuse connection
session = requests.Session()

TP_SELECT = 0
TP_SHOW_TABLE = 1
TP_SHOW_CREATE_TABLE = 2
TP_DESC = 3
TP_USE_DB = 4
TP_OTHER = -1

DEFAULT_HTTP_PROTOCOL = "https://"
DEFAULT_WS_PROTOCOL = "wss://"
EXPORT_LEN = len("\export")
DUMP_INSERT_LEN = len("\insert")

# these are the id of the tabs, it's not important, the history of the tab is lost tho
v_tab_id = "conn_tabs_tab4_tabs_tab1"
v_conn_tab_id = "conn_tabs_tab4"

# auto complete words
completer_candidates = {"exit", "change", "instance", "export", "use", "desc"}

def write_completer_cache():
    p = Path.home() / "omnidb_mug" / "cache.json"
    with open(file=p, mode="w") as f:
        candidates = []
        for w in completer_candidates: candidates.append(w)
        s = json.dumps(candidates)
        f.write(s)

def load_completer_cache() -> bool:
    p = Path.home() / "omnidb_mug" / "cache.json"
    if not os.path.exists(p): return False

    try:
        with open(p, mode='r') as f:
            candidates = json.loads(f.read())
            for c in candidates: add_completer_word(c)
            return True
    except json.JSONDecodeError: pass

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


def escape_quote(s: str, quote: str = "'", escape: str = '\\') -> str:
    '''
    Escape quote with \\ prefix
    '''
    return s.replace(quote, f'{escape}{quote}')


def quote_list(l: list[str]):
    j = []
    for i in range(len(l)): j.append(f"'{escape_quote(l[i])}'")
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

def parse_show_tables_in(sql: str, curr_db: str, debug: bool = False) -> str:
    m = re.search(r"^show +tables +in ([^ ;]+).*", sql, re.IGNORECASE)
    if debug: print(f"[debug] parse_show_tables_in, sql: {sql}, m: {m}")
    if m: return m.group(1)
    return curr_db

def guess_qry_type(sql: str) -> int:
    if re.match(r"^select .* from.*", sql, re.IGNORECASE): return TP_SELECT
    if re.match(r"^show +tables.*", sql, re.IGNORECASE): return TP_SHOW_TABLE
    if re.match(r"^desc .*", sql, re.IGNORECASE): return TP_DESC
    if re.match(r"^show +create +table.*", sql, re.IGNORECASE): return TP_SHOW_CREATE_TABLE
    if re.match(r"^use .*", sql, re.IGNORECASE): return TP_USE_DB
    return TP_OTHER


def auto_complete_db(sql: str, database: str, benchmark: bool = True) -> str:
    '''
    Auto complete schema names for simple queries
    '''
    start = time.monotonic_ns()
    if not database: return sql

    sql = sql.strip()

    completed = False
    re.match
    # single join
    # ^(?:explain)? *select .* from (\.?)(?:[\`\w_]+) *(?:(?:left|right|inner|outer) +join (\.?)(?:[\`\w_]+) (?:using +\(\w+\)|on +[\w\.]+ *\= *[\w\.]+))* ?(?: *| +.*);?$
    # there are 4 joins in this regex
    pat = re.compile(r"^(?:explain)? *select .* from (\.?)(?:[\`\w_]+) *(?:(?:left|right|inner|outer) +join (\.?)(?:[\`\w_]+) (?:using +\(\w+\)|on +[\w\.]+ *\= *[\w\.]+))? *(?:(?:left|right|inner|outer) +join (\.?)(?:[\`\w_]+) (?:using +\(\w+\)|on +[\w\.]+ *\= *[\w\.]+))? *(?:(?:left|right|inner|outer) +join (\.?)(?:[\`\w_]+) (?:using +\(\w+\)|on +[\w\.]+ *\= *[\w\.]+))? *(?:(?:left|right|inner|outer) +join (\.?)(?:[\`\w_]+) (?:using +\(\w+\)|on +[\w\.]+ *\= *[\w\.]+))? ?(?: *| +.*);?$", re.IGNORECASE)
    def repl(m: re.Match):
        idx = []
        for p in pat.groupindex.items():
            idx.append(p.index)

        text = m.group()
        chunks = []
        lastindex = 0
        for i in range(1, pat.groups+1):

            v: str = m.group(i)
            if v is None: continue
            if v.endswith("."): v = database + v
            else: v = database + v + "."

            chunks.append(text[lastindex:m.start(i)])
            chunks.append(v)
            lastindex = m.end(i)

        chunks.append(text[lastindex:])
        # print(chunks)
        return ''.join(chunks)

    copy = re.sub(pat, repl, sql)
    if copy != sql:
        completed = True
        sql = copy

    if not completed:
        m = re.match(r"^show +tables( *)(?:| +like [^ ]+) *;? *$", sql, re.IGNORECASE)
        if m:
            open, close = m.span(1)
            sql = sql[: open] + f" in {database}" + sql[close:]
            completed = True

    if not completed:
        m = re.match(r"^show +create +table +([`0-9a-zA-Z_]+) *;? *$", sql, re.IGNORECASE)
        if m:
            open, close = m.span(1)
            sql = insert_db_name(sql, database, open, close)
            completed = True

    if not completed:
        m = re.match(r"^desc +(\.?[`0-9a-zA-Z_]+) *;? *$", sql, re.IGNORECASE)
        if m:
            open, close = m.span(1)
            sql = insert_db_name(sql, database, open, close)
            completed = True

    if benchmark and completed: print(f"Auto-completed ({(time.monotonic_ns() - start) / 1e6:.3f}ms): {sql}")
    return sql


def parse_pretty_print(sql: str) -> tuple[bool, str]:
    m = re.match(r"^.*(\\G) *;?$", sql, re.IGNORECASE)
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


def is_show_create_table(sql: str) -> bool:
    return re.match(r"^show +create +table +[\.`0-9a-zA-Z_]+ *;? *$", sql, re.IGNORECASE)


def is_select(cmd: str) -> bool:
    return re.match(r"^select.*$", cmd, re.IGNORECASE)


def is_exit(cmd: str) -> bool:
    return re.match(r"^(?:quit|exit|\\quit|\\exit)(?:|\(\))$", cmd, re.IGNORECASE)


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


def escape(sql: str) -> str:
    return re.sub(r'([^\\])(")', r'\1\\"', sql)


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


class BufWriter():

    def __init__(self):
        self.res = ""

    def print(self, s = ""):
        self.res = self.res + s + "\n"

    def write_log(self):
        print(self.res)

    def write_file(self, file):
        if file: file.write(self.res)


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

    w = BufWriter()

    if not slient and len(col) > 0:
        if is_show_create_table(sql):
            w.print("\n" + rows[0][1])
        else:
            if pretty:

                # max length among the column names
                max_col_len = 0
                for c in col: max_col_len = max(str_width(c), max_col_len)
                if debug: w.print(f"[debug] max_col_len: {max_col_len}")

                sl = []
                for r in rows:
                    output = ""
                    for i in range(len(col)):
                        output += f"{spaces(max_col_len - str_width(col[i]))}{col[i]}: {r[i]}"
                        if i < len(col) - 1: output += "\n"
                    sl.append(output)

                w.print("\n******************************************\n")
                w.print("\n\n******************************************\n\n".join(sl))
                w.print("\n******************************************")
            else:
                # max length among the rows
                indent : dict[int][int] = {}
                for i in range(len(col)): indent[i] = str_width(col[i])
                for r in rows:
                    for i in range(len(col)): indent[i] = max(indent[i], str_width(r[i]))

                w.print()
                col_title = "| "
                col_sep = "|-"
                for i in range(len(col)):
                    col_title += col[i] + spaces(indent[i] - str_width(col[i]) + 1) + " | "
                    col_sep += sjoin(indent[i] + 1, "-") + "-|"
                    if i < len(col) - 1: col_sep += "-"
                w.print(col_sep + "\n" + col_title + "\n" + col_sep)

                for r in rows:
                    row_ctn = "| "
                    for i in range(len(col)): row_ctn += r[i] + spaces(1 + indent[i] - str_width(r[i])) + " | "
                    w.print(row_ctn)
                w.print(col_sep)

        w.print()
        w.print(f"Total    : {len(rows)}")
        w.print(f"Cost     : {cost}")
        w.print(f"Wall Time: {(time.monotonic_ns() - start) / 1e6:.2f} ms")
        w.print()

    w.write_log()
    w.write_file(qc.logf)

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


def flatten(ll: list[list]) -> list:
    flat = []
    for r in ll:
        for v in r: flat.append(v)
    return flat

def nested_add_completer_word(rl: list[list[str]], debug = False):
    for r in rl:
        for v in r: add_completer_word(v, debug)


def add_completer_word(word, debug = False):
    global completer_candidates
    if debug: print(f"[debug] add completer: {word}")
    if not word: return
    completer_candidates.add(word)


def get_platform():
    # https://stackoverflow.com/questions/434597/open-document-with-default-os-application-in-python-both-in-windows-and-mac-os
    if sys.platform == 'linux':
        try:
            proc_version = open('/proc/version').read()
            if 'Microsoft' in proc_version:
                return 'wsl'
        except:
            pass
    return sys.platform


def open_with_default_app(filename):
    # https://stackoverflow.com/questions/434597/open-document-with-default-os-application-in-python-both-in-windows-and-mac-os
    platform = get_platform()
    if platform == 'darwin':
        subprocess.call(('open', filename))
    elif platform in ['win64', 'win32']:
        os.startfile(filename.replace('/','\\'))
    elif platform == 'wsl':
        subprocess.call('cmd.exe /C start'.split() + [filename])
    else:                                   # linux variants
        subprocess.call(('xdg-open', filename))


def select_instance(sh: OSession, qc: QueryContext, select_first = False) -> QueryContext:
    debug = qc.debug

    qc.v_context_code += 1
    if debug: print(f"[debug] QueryContext.v_context_code: {qc.v_context_code}")

    # list database instance, pick one to use
    db = get_database_list(sh, debug=debug)
    v_tab_db_id = db.tabs[0].tab_db_id
    v_db_index = db.tabs[0].index # this is the previously selected v_conn_id

    print("Available database connections:")
    prev_selected = 0
    for i in range(len(db.connections)):
        if db.connections[i].v_conn_id  == v_db_index:
            prev_selected = i
            print(f"* [{i}] '{db.connections[i].v_alias}'")
        else: print(f"  [{i}] '{db.connections[i].v_alias}'")

    if select_first: resp = prev_selected
    else:
        resp = input(f"\nPlease select database connections: ").strip().lower()
        if not resp: resp = prev_selected  # default db connection

    selected_idx = int(resp)

    # invalid answer, select the previous one
    if selected_idx < 0 or selected_idx >= len(db.connections): selected_idx = prev_selected

    # change to selected connection
    selected_conn = db.connections[selected_idx]

    # change active database
    v_db_index = selected_conn.v_conn_id
    change_active_database(sh, v_db_index, v_conn_tab_id, "", debug=debug)
    print(f'Selected database \'{selected_conn.v_alias}\'')

    qc.v_tab_db_id = v_tab_db_id
    qc.v_db_index = v_db_index
    return qc


def parse_use_db(sql: str) -> tuple[bool, str]:
    m = re.match(r"^use *([0-9a-zA-Z_]*) *;?$", sql, re.IGNORECASE)
    if m:
        use_database = m.group(1).strip()
        return True, use_database
    return False, None


def export(rows, cols, outf):
    import importlib.util
    found = importlib.util.find_spec("excelparser") is not None
    if not found:
        print("\nYou should install https://github.com/CurtisNewbie/excelparser before using the '\export' command\n")
        return

    # https://github.com/CurtisNewbie/excelparser
    import excelparser
    ep = excelparser.ExcelParser(outf)
    ep.rows = rows
    ep.cols = cols
    ep.export(outf)
    print(f"Exported to '{abspath(outf)}'")
    open_with_default_app(outf)


def is_reconnect(cmd: str) -> bool:
    return re.match("^\\\\reconnect.*", cmd, re.IGNORECASE)


def is_change_instance(cmd: str) -> bool:
    return re.match("^\\\\change.*", cmd, re.IGNORECASE)  # cmd is trimmed already


def is_dump_insert_cmd(cmd: str) -> tuple[bool, str]:
    m = re.match("^\\\\insert(.*)", cmd, re.IGNORECASE)  # cmd is trimmed already
    if not m:
       return False, ""
    return True, m.group(1).strip()


def is_export_cmd(cmd: str) -> tuple[bool, str]:
    m = re.match("^\\\\export(.*)", cmd, re.IGNORECASE)  # cmd is trimmed already
    if not m:
       return False, ""
    return True, m.group(1).strip()


def is_debug(cmd: str) -> str:
    return re.match("^\\\\debug.*", cmd, re.IGNORECASE)  # cmd is trimmed already


def load_password(pf: str) -> str:
    with open(pf) as f: return f.read().strip()


def completer(text, state):
    global completer_candidates
    if not text: return None
    options = [cmd for cmd in completer_candidates if cmd.startswith(text)]
    if state < len(options): return options[state]
    else: return None


def launch_console(args):
    global v_tab_id, v_conn_tab_id

    host = args.host # host (without protocol)
    uname = args.user # username
    batch_export_limit = args.batch_export_limit # page limit for batch export
    http_protocol = args.http_protocol # http protocol
    ws_protocol = args.ws_protocol # websocket protocol
    force_batch_export = args.force_batch_export # whether to force to use OFFSET,LIMIT to export
    debug = args.debug # enable debug mode
    batch_export_throttle_ms = args.batch_export_throttle_ms # sleep time in ms for each batch export
    insert_excl = args.insert_excl.strip()
    if not insert_excl: insert_excl = "id"
    insert_excl_cols: set[str] = set(insert_excl.split(","))
    multiline_input = not args.oneline_input

    env_print("Python Version", sys.version)
    env_print("Using HTTP Protocol", http_protocol)
    env_print("Using WebSocket Protocol", ws_protocol)
    env_print("Force Batch Export (OFFSET, LIMIT)", force_batch_export)
    env_print("Log File", args.log)
    env_print("AutoCompleter Cache", Path.home() / "omnidb_mug" / "cache.json")
    env_print("Excluded Columns for INSERT Dump", insert_excl)
    env_print("Multi-line Console Input", multiline_input)

    ws: WebSocket = None
    qry_ctx = QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id
    qry_ctx.debug = debug

    completer_cache_loaded = load_completer_cache()

    try:
        while not host: input("Enter host of Omnidb: ")
        env_print("Using Host", host)

        while not uname: uname = input("Enter Username: ")
        env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        if args.password: pw = args.password
        elif args.passwordfile: pw = load_password(args.passwordfile)
        while not pw: pw = getpass.getpass(f"Enter Password for '{uname}': ").strip()

        # login
        sh = login(csrf, host, uname, pw, protocol=http_protocol, debug=debug)

        # list database, pick one to use
        qry_ctx = select_instance(sh, qry_ctx, select_first=True)

        # connect websocket
        ws = ws_connect(sh, host, debug=debug, protocol=ws_protocol)

    except KeyboardInterrupt:
        close_ws(ws=ws, debug=debug)
        print("\nBye!")
        return

    # setup word completer
    readline.parse_and_bind("tab: complete")
    readline.set_completer(completer)

    # execute queries
    print()
    print("Switching to interactive mode, type 'quit' to exit")
    print()
    print(" \\export SELECT_SQL    export results as an excel file")
    print(" \\insert SELECT_SQL    generate INSERT SQL")
    print(" \\change               change the connected instance")
    print(" \\reconnect            reconnect the websocket connection")
    print(" \\debug                enable/disable debug mode")
    print()

    if not completer_cache_loaded:
        # fetch all schema names for completer
        if debug: print("[debug] Fetching schema names for auto-completion")
        ok, _, rows = exec_query(ws=ws, sql="SHOW DATABASES", qc=qry_ctx, slient=True)
        if ok: nested_add_completer_word(rows, debug) # feed SCHEMA names

    # database names that we have USE(d)
    swapped_db = set()
    curr_db = ""

    # warmup auto_complete_db, initialized all the regexp
    auto_complete_db("WARMUP", "NONE", False)

    logf = None
    if args.log: logf = open(mode='a', file=args.log, buffering=1)
    qry_ctx.logf = logf

    while True:
        try:
            if not multiline_input:
                cmd = input(f"({curr_db}) > " if curr_db else "> ").strip()
                if cmd == "": continue
                if is_exit(cmd):
                    write_completer_cache()
                    break
            else:
                cmd = ""
                inputs : list[str] = []
                exit_console = False
                while True:
                    prompt = f"({curr_db}) > " if curr_db else "> "
                    if len(inputs) > 0: prompt = "  "
                    cmd = input(prompt).strip()
                    if cmd == "":
                        if len(inputs) < 1: continue
                        break
                    if is_exit(cmd):
                        write_completer_cache()
                        exit_console = True
                        break

                    inputs.append(cmd)
                    if is_debug(cmd) or is_change_instance(cmd) or is_reconnect(cmd): break
                    if cmd.endswith(';'): break
                    is_pretty_print, _ = parse_pretty_print(cmd)
                    if is_pretty_print: break

                if exit_console: break
                for i in range(len(inputs)): inputs[i] = inputs[i].strip()
                cmd = " ".join(inputs)

            batch_export = False
            sql = cmd

            # parse \G
            is_pretty_print, sql = parse_pretty_print(sql)

            if is_debug(cmd):
                pre = "Disabled" if qry_ctx.debug else "Enabled"
                print(f"{pre} debug mode")
                qry_ctx.debug = not qry_ctx.debug
                debug = not debug
                continue

            # TODO refactor these parse command stuff :(
            do_export, export_sql = is_export_cmd(cmd)
            if do_export: # parse \export command
                sql = export_sql
                if sql == "": continue

            dump_insert, dump_sql = is_dump_insert_cmd(cmd)
            if dump_insert: # parse \insert command
                sql = dump_sql
                if sql == "": continue

            if is_change_instance(cmd): # parse \change command
                qry_ctx = select_instance(sh, qry_ctx)
                continue

            if is_reconnect(cmd): # parse \reconnect command
                print("Reconnecting...")
                try:
                    ws.close()
                except Exception as e:
                    if debug: print(f"[debug] error occurred while reconnecting, {e}")
                    pass # do nothing

                ws = ws_connect(sh, host, debug=debug, protocol=ws_protocol)
                print("Reconnected")
                continue

            # guess the type of the sql query, may be redundant, but it's probably more maintainable :D
            qry_tp: int = guess_qry_type(sql)
            if debug: print(f"[debug] qry_type: {qry_tp}")

            # is export & select
            if do_export and qry_tp == TP_SELECT:
                if force_batch_export: batch_export = True
                elif not query_has_limit(sql):
                    batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'

            if debug: print(f"[debug] sql: '{sql}'")

            if qry_tp == TP_USE_DB: # USE `mydb`
                ok, db = parse_use_db(sql)
                if ok:
                    curr_db = db
                    if not db or db in swapped_db: continue
                    print("Fetching table names for auto-completion")
                    ok, _, drows = exec_query(ws=ws, sql=f"SHOW TABLES IN {db}", qc=qry_ctx, slient=True)
                    if not ok: continue
                    for ro in drows:
                        for r in ro:
                            add_completer_word(r, debug)
                            add_completer_word(f"{db}.{r}", debug)
                    swapped_db.add(db)
                    continue

            # complete schema name for simple queries
            if curr_db: sql = auto_complete_db(sql, db)

            def_outf = None
            if do_export: def_outf = "export_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
            if batch_export:
                outf = input(f'Please specify where to export (defaults to \'{def_outf}\'): ').strip()
                if not outf: outf = def_outf

                ok, acc_cols, acc_rows = exec_batch_query(ws=ws,
                                                               sql=sql,
                                                               qry_ctx=qry_ctx,
                                                               page_size=batch_export_limit,
                                                               throttle_ms=batch_export_throttle_ms,
                                                               slient=False)
                if not ok: continue
                export(acc_rows, acc_cols, outf) # all queries are finished, export them

            else:
                ok, cols, rows = exec_query(ws=ws, sql=sql, qc=qry_ctx, slient=False, pretty=is_pretty_print)
                if not ok: continue
                if do_export:
                    outf = input(f'Please specify where to export (defaults to \'{def_outf}\'): ').strip()
                    if not outf: outf = def_outf
                    export(rows, cols, outf)
                elif qry_tp == TP_SELECT and dump_insert:
                    dump_insert_sql(sql, cols, rows, insert_excl_cols)

                # feed the table name and field names to completer
                if qry_tp == TP_SHOW_TABLE:
                    _db: str = parse_show_tables_in(sql, curr_db, debug)
                    if not _db: _db = curr_db

                    for v in flatten(rows):
                        add_completer_word(v, debug)
                        add_completer_word(f"{_db}.{v}", debug)
                elif qry_tp == TP_DESC:
                    for ro in rows: add_completer_word(ro[0], debug) # ro[0] is `Field`
        except KeyboardInterrupt: print()
        except BrokenPipeError:
            print("\nReconnecting...")
            ws = ws_connect(sh, host, debug=debug, protocol=ws_protocol)
            print("Reconnected, please try again")

    close_ws(ws) # disconnect websocket
    if logf != None: logf.close() # close log file
    print("Bye!")


def load_script(args) -> list[str]:
    with open(args.script) as f:
        return f.readlines()


def run_scripts(args):
    scripts = load_script(args)

    global v_tab_id, v_conn_tab_id

    host = args.host # host (without protocol)
    uname = args.user # username
    http_protocol = args.http_protocol # http protocol
    ws_protocol = args.ws_protocol # websocket protocol
    debug = args.debug # enable debug mode
    os.makedirs(Path.home() / "omnidb_mug", exist_ok=True)

    env_print("Using HTTP Protocol", http_protocol)
    env_print("Using WebSocket Protocol", ws_protocol)
    env_print("Debug Mode", debug)
    env_print("Log File", args.log)

    ws: WebSocket = None
    qry_ctx = QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id
    qry_ctx.debug = debug

    try:
        while not host: input("Enter host of Omnidb: ")
        env_print("Using Host", host)

        while not uname: uname = input("Enter Username: ")
        env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        if args.password: pw = args.password
        elif args.passwordfile: pw = load_password(args.passwordfile)
        while not pw: pw = getpass.getpass(f"Enter Password for '{uname}': ").strip()

        # login
        sh = login(csrf, host, uname, pw, protocol=http_protocol, debug=debug)

        # list database, pick one to use
        qry_ctx = select_instance(sh, qry_ctx, select_first=True)

        # connect websocket
        ws = ws_connect(sh, host, debug=debug, protocol=ws_protocol)

    except KeyboardInterrupt:
        close_ws(ws=ws, debug=debug)
        print("\nBye!")
        return

    # execute queries
    print("\nSwitching to scripting mode\n")

    logf = None
    if args.log: logf = open(mode='a', file=args.log, buffering=1)
    qry_ctx.logf = logf

    for i in range(len(scripts)):
        try:
            cmd = scripts[i].strip()
            if cmd == "": continue
            if is_exit(cmd): break
            export, _ = is_export_cmd(cmd)
            if export: continue  # export command is not supported
            if is_change_instance(cmd): continue # change instance not supported
            if is_reconnect(cmd): continue # \reconnect not supported

            sql = cmd
            if sql.startswith("--"): continue # commented

            # parse \G
            is_pretty_print, sql = parse_pretty_print(sql)

            # guess the type of the sql query, may be redundant, but it's probably more maintainable :D
            qry_tp: int = guess_qry_type(sql)
            if qry_tp == TP_USE_DB: continue # USE `mydb` is not supported

            if debug: print(f"[debug] sql: '{sql}'")

            print(f"> Executing - {sql}")
            exec_query(ws=ws, sql=sql, qc=qry_ctx, slient=False, pretty=is_pretty_print)

        except KeyboardInterrupt: break
        except BrokenPipeError:
            print("\nDisconnected...")
            return

    # disconnect websocket
    close_ws(ws)
    print("Bye!")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="OmniDB Mug By Yongjie.Zhuang", formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--host', type=str, help=f"Host", default="")
    ap.add_argument('-u', '--user', type=str, help=f"User", default="")
    ap.add_argument('-p', '--password', type=str, help=f"Password", default="")
    ap.add_argument('-pf', '--passwordfile', type=str, help=f"Password file", default="")
    ap.add_argument('-d', '--debug', help=f"Enable debug mode", action="store_true")
    ap.add_argument('--http-protocol', type=str, help=f"HTTP Protocol to use (default: {DEFAULT_HTTP_PROTOCOL})", default=DEFAULT_HTTP_PROTOCOL)
    ap.add_argument('--ws-protocol', type=str, help=f"WebSocket Protocol to use (default: {DEFAULT_WS_PROTOCOL})", default=DEFAULT_WS_PROTOCOL)
    ap.add_argument('--force-batch-export', help=f"Force to use batch export (offset/limit)", action="store_true")
    ap.add_argument('--batch-export-limit', type=int, help=f"Batch export limit (default: {400})", default=400)
    ap.add_argument('--batch-export-throttle-ms', type=int, help=f"Batch export throttle time in ms (default: {1500})", default=1500)
    ap.add_argument('--script', type=str, help=f"Path to scripting file", default="")
    defaultLogFile = p = Path.home() / "omnidb_mug" / "exec.log"
    ap.add_argument('--log', type=str, help=f"Path to log file, both SQLs and results are logged(default: ${defaultLogFile})", default=defaultLogFile)
    ap.add_argument('--insert-excl', type=str, help=f"Exclude columns when trying to dump INSERT sqls (delimited by \',\')", default="")
    ap.add_argument('--oneline-input', help=f"Disable multi-line console input", action="store_true")
    args = ap.parse_args()

    if args.script: run_scripts(args)
    else: launch_console(args)