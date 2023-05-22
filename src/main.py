import datetime
import argparse
import util
import getpass
import os
import sys
import re
import readline # don't remove this, this is for input()
import time
from websocket import WebSocket
import time
import subprocess
from os.path import abspath

EXPORT_LEN = len("\export")

# TODO these doesn't seem to be important :D
v_tab_id = "conn_tabs_tab4_tabs_tab1"
v_conn_tab_id = "conn_tabs_tab4"

# auto complete words
completer_candidates = {"exit", "change", "instance", "export", "use", "desc"}

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

def select_instance(sh: util.OSession, qc: util.QueryContext, select_first = False) -> util.QueryContext:
    debug = qc.debug

    qc.v_context_code += 1
    if debug: print(f"[debug] QueryContext.v_context_code: {qc.v_context_code}")

    # list database instance, pick one to use
    db = util.get_database_list(sh, debug=debug)
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
    util.change_active_database(sh, v_db_index, v_conn_tab_id, "", debug=debug)
    print(f'Selected database \'{selected_conn.v_alias}\'')

    qc.v_tab_db_id = v_tab_db_id
    qc.v_db_index = v_db_index
    return qc


use_db_sql_pat =  re.compile(r"^use *([0-9a-zA-Z_]*) *;?$", re.IGNORECASE)
def parse_use_db(sql: str) -> tuple[bool, str]:
    m = use_db_sql_pat.match(sql)
    if m:
        use_database = m.group(1).strip()
        # print(f"Using databse: '{use_database}'")
        return True, use_database
    return False, None


def export(rows, cols, outf):
    # https://github.com/CurtisNewbie/excelparser
    import excelparser
    ep = excelparser.ExcelParser(outf)
    ep.rows = rows
    ep.cols = cols
    ep.export(outf)
    print(f"Exported to '{abspath(outf)}'")
    open_with_default_app(outf)

re_conn_pat = re.compile("^\\\\reconnect.*", re.IGNORECASE)
def is_reconnect(cmd: str) -> str:
    return re_conn_pat.match(cmd)  # cmd is trimmed already

chg_inst_pat = re.compile("^\\\\change.*", re.IGNORECASE)
def is_change_instance(cmd: str) -> str:
    return chg_inst_pat.match(cmd)  # cmd is trimmed already

exp_cmd_pat = re.compile("^\\\\export.*", re.IGNORECASE)
def is_export_cmd(cmd: str) -> str:
    return exp_cmd_pat.match(cmd)  # cmd is trimmed already

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

    util.env_print("Using HTTP Protocol", http_protocol)
    util.env_print("Using WebSocket Protocol", ws_protocol)
    util.env_print("Force Batch Export (OFFSET, LIMIT)", force_batch_export)
    util.env_print("Debug Mode", debug)
    util.env_print("Log File", args.log)

    ws: WebSocket = None
    qry_ctx = util.QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id
    qry_ctx.debug = debug

    try:
        while not host: input("Enter host of Omnidb: ")
        util.env_print("Using Host", host)

        while not uname: uname = input("Enter Username: ")
        util.env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = util.get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        if args.password: pw = args.password
        elif args.passwordfile: pw = load_password(args.passwordfile)
        while not pw: pw = getpass.getpass(f"Enter Password for '{uname}': ").strip()

        # login
        sh = util.login(csrf, host, uname, pw, protocol=http_protocol, debug=debug)

        # list database, pick one to use
        qry_ctx = select_instance(sh, qry_ctx, select_first=True)

        # connect websocket
        ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)

    except KeyboardInterrupt:
        util.close_ws(ws=ws, debug=debug)
        print("\nBye!")
        return

    # setup word completer
    readline.parse_and_bind("tab: complete")
    readline.set_completer(completer)

    # execute queries
    print()
    print("Switching to interactive mode, enter 'quit()' or 'quit' or 'exit' to exit")
    print("Enter '\\export [SQL]' to export results as an excel file")
    print("Enter '\\change' to change the connected instance")
    print("Enter '\\reconnect' to reconnect the websocket connection")
    print()

    # fetch all schema names for completer
    print("Fetching schema names for auto-completion")
    ok, _, rows = util.exec_query(ws=ws, sql="SHOW DATABASES", qc=qry_ctx, slient=True)
    if ok: nested_add_completer_word(rows, debug) # feed SCHEMA names

    # database names that we have USE(d)
    swapped_db = set()

    logf = None
    if args.log: logf = open(mode='a', file=args.log, buffering=1)
    qry_ctx.logf = logf

    while True:
        try:
            cmd = input("> ").strip()
            if cmd == "": continue
            if util.is_exit(cmd): break

            batch_export = False
            sql = cmd

            # parse \G
            is_pretty_print, sql = util.parse_pretty_print(sql)

            # parse export command
            do_export = is_export_cmd(cmd)
            if do_export:
                sql: str = sql[EXPORT_LEN:].strip()
                if sql == "": continue

            # guess the type of the sql query, may be redundant, but it's probably more maintainable :D
            qry_tp: int = util.guess_qry_type(sql)

            # is export & select
            if do_export and qry_tp == util.TP_SELECT:
                if force_batch_export: batch_export = True
                elif not util.query_has_limit(sql):
                    batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'

            if is_change_instance(cmd):
                qry_ctx = select_instance(sh, qry_ctx)
                continue

            if is_reconnect(cmd):
                print("Reconnecting...")
                try:
                    ws.close()
                except Exception as e:
                    if debug: print(f"[debug] error occurred while reconnecting, {e}")
                    pass # do nothing

                ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)
                print("Reconnected")
                continue

            if debug: print(f"[debug] sql: '{sql}'")

            if qry_tp == util.TP_USE_DB: # USE `mydb`
                ok, db = parse_use_db(sql)
                if ok:
                    if not db or db in swapped_db: continue
                    print("Fetching table names for auto-completion")
                    ok, _, drows = util.exec_query(ws=ws, sql=f"SHOW TABLES IN {db}", qc=qry_ctx, slient=True)
                    if not ok: continue
                    for ro in drows:
                        for r in ro:
                            add_completer_word(r, debug)
                            add_completer_word(f"{db}.{r}", debug)
                    swapped_db.add(db)
                    continue

            def_outf = None
            if do_export: def_outf = "export_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
            if batch_export:
                outf = input(f'Please specify where to export (defaults to \'{def_outf}\'): ').strip()
                if not outf: outf = def_outf

                ok, acc_cols, acc_rows = util.exec_batch_query(ws=ws,
                                                               sql=sql,
                                                               qry_ctx=qry_ctx,
                                                               page_size=batch_export_limit,
                                                               throttle_ms=batch_export_throttle_ms,
                                                               slient=False)
                if not ok: continue
                export(acc_rows, acc_cols, outf) # all queries are finished, export them

            else:
                ok, cols, rows = util.exec_query(ws=ws, sql=sql, qc=qry_ctx, slient=False, pretty=is_pretty_print)
                if not ok: continue
                if do_export:
                    outf = input(f'Please specify where to export (defaults to \'{def_outf}\'): ').strip()
                    if not outf: outf = def_outf
                    export(rows, cols, outf)

                # feed the table name and field names to completer
                if qry_tp == util.TP_SHOW_TABLE: nested_add_completer_word(rows, debug)
                elif qry_tp == util.TP_DESC:
                    for ro in rows: add_completer_word(ro[0], debug) # ro[0] is `Field`
        except KeyboardInterrupt: print()
        except BrokenPipeError:
            print("\nReconnecting...")
            ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)
            print("Reconnected, please try again")

    util.close_ws(ws) # disconnect websocket
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

    util.env_print("Using HTTP Protocol", http_protocol)
    util.env_print("Using WebSocket Protocol", ws_protocol)
    util.env_print("Debug Mode", debug)
    util.env_print("Log File", args.log)

    ws: WebSocket = None
    qry_ctx = util.QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id
    qry_ctx.debug = debug

    try:
        while not host: input("Enter host of Omnidb: ")
        util.env_print("Using Host", host)

        while not uname: uname = input("Enter Username: ")
        util.env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = util.get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        if args.password: pw = args.password
        elif args.passwordfile: pw = load_password(args.passwordfile)
        while not pw: pw = getpass.getpass(f"Enter Password for '{uname}': ").strip()

        # login
        sh = util.login(csrf, host, uname, pw, protocol=http_protocol, debug=debug)

        # list database, pick one to use
        qry_ctx = select_instance(sh, qry_ctx, select_first=True)

        # connect websocket
        ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)

    except KeyboardInterrupt:
        util.close_ws(ws=ws, debug=debug)
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
            if util.is_exit(cmd): break
            if is_export_cmd(cmd): continue  # export command is not supported
            if is_change_instance(cmd): continue # change instance not supported
            if is_reconnect(cmd): continue # \reconnect not supported

            sql = cmd
            if sql.startswith("--"): continue # commented

            # parse \G
            is_pretty_print, sql = util.parse_pretty_print(sql)

            # guess the type of the sql query, may be redundant, but it's probably more maintainable :D
            qry_tp: int = util.guess_qry_type(sql)
            if qry_tp == util.TP_USE_DB: continue # USE `mydb` is not supported

            if debug: print(f"[debug] sql: '{sql}'")

            print(f"> Executing - {sql}")
            util.exec_query(ws=ws, sql=sql, qc=qry_ctx, slient=False, pretty=is_pretty_print)

        except KeyboardInterrupt: break
        except BrokenPipeError:
            print("\nDisconnected...")
            return

    # disconnect websocket
    util.close_ws(ws)
    print("Bye!")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="OmniDB Mug By Yongjie.Zhuang", formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--host', type=str, help=f"Host", default="")
    ap.add_argument('-u', '--user', type=str, help=f"User", default="")
    ap.add_argument('-p', '--password', type=str, help=f"Password", default="")
    ap.add_argument('-pf', '--passwordfile', type=str, help=f"Password file", default="")
    ap.add_argument('-d', '--debug', help=f"Enable debug mode", action="store_true")
    ap.add_argument('--http-protocol', type=str, help=f"HTTP Protocol to use (default: {util.DEFAULT_HTTP_PROTOCOL})", default=util.DEFAULT_HTTP_PROTOCOL)
    ap.add_argument('--ws-protocol', type=str, help=f"WebSocket Protocol to use (default: {util.DEFAULT_WS_PROTOCOL})", default=util.DEFAULT_WS_PROTOCOL)
    ap.add_argument('--force-batch-export', help=f"Force to use batch export (offset/limit)", action="store_true")
    ap.add_argument('--batch-export-limit', type=int, help=f"Batch export limit (default: {400})", default=400)
    ap.add_argument('--batch-export-throttle-ms', type=int, help=f"Batch export throttle time in ms (default: {1000})", default=1000)
    ap.add_argument('--script', type=str, help=f"Path to scripting file", default="")
    ap.add_argument('--log', type=str, help=f"Path to log file (only SQLs are logged)", default="")
    args = ap.parse_args()

    if args.script: run_scripts(args)
    else: launch_console(args)