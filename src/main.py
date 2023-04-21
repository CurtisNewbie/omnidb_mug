import argparse
import util
import getpass
import os
import re
import readline # don't remove this, this is for input()
import time
from websocket import WebSocket
import time

EXPORT_LEN = len("export")
CHANGE_INSTANCE = "change instance"

# TODO these doesn't seem to be important :D
v_tab_id = "conn_tabs_tab4_tabs_tab1"
v_conn_tab_id = "conn_tabs_tab4"

# auto complete words
completer_candidates = {"exit", "change", "instance", "export", "use", "desc"}

def nested_add_completer_word(rl: list[list[str]]):
    for r in rl:
        for v in r: add_completer_word(v)

def add_completer_word(word):
    global completer_candidates
    completer_candidates.add(word)


def select_instance(sh: util.OSession, qc: util.QueryContext, select_first = False, debug = False) -> util.QueryContext:
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

    selected_conn = db.connections[int(resp)]

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
    print(f"Exported to '{outf}'")


chg_inst_pat = re.compile("[Cc][Hh][Aa][Nn][Gg][Ee] +[Ii][Nn][Ss][Tt][Aa][Nn][Cc][Ee] *")
def is_change_instance(cmd: str) -> str:
    return chg_inst_pat.match(cmd)  # cmd is trimmed already


exp_cmd_pat = re.compile("[Ee][Xx][Pp][Oo][Rr][Tt].*")
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

def launch_console():
    global v_tab_id, v_conn_tab_id
    use_database = ""

    ap = argparse.ArgumentParser(description="OmniDB Mug By Yongjie.Zhuang", formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--host', type=str, help=f"Host", default="")
    ap.add_argument('-u', '--user', type=str, help=f"User", default="")
    ap.add_argument('-p', '--password', type=str, help=f"Password", default="")
    ap.add_argument('-pf', '--passwordfile', type=str, help=f"Password file", default="")
    ap.add_argument('-d', '--debug', help=f"Enable debug mode (true/false)", action="store_true")
    ap.add_argument('--http-protocol', type=str, help=f"HTTP Protocol to use (default: {util.DEFAULT_HTTP_PROTOCOL})", default=util.DEFAULT_HTTP_PROTOCOL)
    ap.add_argument('--ws-protocol', type=str, help=f"WebSocket Protocol to use (default: {util.DEFAULT_WS_PROTOCOL})", default=util.DEFAULT_WS_PROTOCOL)
    ap.add_argument('--force-batch-export', help=f"Force to use batch export (offset/limit)", action="store_true")
    ap.add_argument('--batch-export-limit', type=int, help=f"Batch export limit (default: {400})", default=400)
    ap.add_argument('--batch-export-throttle-ms', type=int, help=f"Batch export throttle time in ms (default: {200})", default=200)
    ap.add_argument('--disable-db-auto-complete', help=f"Disable DB name autocomplete", action="store_true")
    args = ap.parse_args()

    host = args.host # host (without protocol)
    uname = args.user # username
    batch_export_limit = args.batch_export_limit # page limit for batch export
    http_protocol = args.http_protocol # http protocol
    ws_protocol = args.ws_protocol # websocket protocol
    disable_db_auto_complete = True if args.disable_db_auto_complete else False # whether to disable db name autocomplete
    force_batch_export = True if args.force_batch_export else False # whether to force to use OFFSET,LIMIT to export
    debug = True if args.debug else False # enable debug mode
    batch_export_throttle_ms = args.batch_export_throttle_ms # sleep time in ms for each batch export

    util.env_print("Using HTTP Protocol", http_protocol)
    util.env_print("Using WebSocket Protocol", ws_protocol)
    util.env_print("Force Batch Export (OFFSET, LIMIT)", force_batch_export)
    util.env_print("Debug Mode", debug)
    util.env_print("Disable DB Name Auto-Complete", disable_db_auto_complete)

    ws: WebSocket = None
    qry_ctx = util.QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id

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
        qry_ctx = select_instance(sh, qry_ctx, select_first=True, debug=debug)

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
    print("Enter 'export [SQL]' to export excel files (csv/xlsx/xls)")
    print("Enter 'change instance' to change the connected instance")
    print()

    qry_ctx.v_context_code = 2 # start with two, when we connect websocket, we always send the first msg to server right away

    # fetch all schema names for completer
    qry_ctx.v_context_code += 1
    ok, cols, rows = util.exec_query(ws, "show databases", qry_ctx, debug, True)
    if ok: nested_add_completer_word(rows)

    # database names that we have USE(d)
    swapped_db = set()

    while True:
        try:
            cmd = input(f"({use_database}) > " if use_database else "> ").strip()
            if cmd == "": continue
            if util.is_exit(cmd): break

            qry_ctx.v_context_code += 1
            batch_export = False
            sql = cmd

            do_export = is_export_cmd(cmd)
            if do_export:
                sql: str = sql[EXPORT_LEN:].strip()
                if sql == "": continue

                if util.is_select(sql):
                    if force_batch_export: batch_export = True
                    elif not util.query_has_limit(sql):
                        batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'

            if is_change_instance(cmd):
                qry_ctx = select_instance(sh, qry_ctx, debug=debug)
                continue

            if debug: print(f"[debug] sql: '{sql}'")

            # guess the type of the sql query, may be redundant, but it's probably more maintainable :D
            qry_tp: int = util.guess_qry_type(sql)

            if qry_tp == util.TP_USE_DB: # USE `mydb`
                ok, db = parse_use_db(sql)
                if ok:
                    # reset the database name used
                    if not db:
                        use_database = ""
                        continue

                    if db in swapped_db:
                        use_database = db
                        continue
                    else:
                        # fetch tables names for completer
                        ok, cols, rows = util.exec_query(ws, f"SHOW TABLES in {db}", qry_ctx, debug, True)
                        if ok:
                            swapped_db.add(db)
                            use_database = db
                            nested_add_completer_word(rows)
                            continue

            # auto complete database name
            elif not disable_db_auto_complete and use_database:
                sql = util.auto_complete_db(sql, use_database)

            if debug: print(f"[debug] preprocessed sql: '{sql}'")
            if batch_export:
                outf = input('Please specify where to export (default to \'export.xlsx\'): ').strip()
                if not outf: outf = "export.xlsx"

                offset = 0
                acc_cols = []
                acc_rows = []

                while True:
                    if sql.endswith(";"): sql = sql[:len(sql) - 1].strip()
                    offset_sql = sql + f" limit {offset}, {batch_export_limit}"
                    print(offset_sql)

                    ok, cols, rows = util.exec_query(ws, offset_sql, qry_ctx, debug)
                    if not ok or len(rows) < 1: break # error or empty page

                    if offset < 1: acc_cols = cols # first page
                    acc_rows += rows # append rows
                    offset += batch_export_limit # next page

                    if len(rows) < batch_export_limit:  break # the end of pagination
                    if batch_export_throttle_ms > 0: time.sleep(batch_export_throttle_ms / 1000) # throttle a bit, not so fast

                export(acc_rows, acc_cols, outf) # all queries are finished, export them

            else:
                ok, cols, rows = util.exec_query(ws, sql, qry_ctx, debug)
                if not ok: continue
                if do_export:
                    outf = input('Please specify where to export (default to \'export.xlsx\'): ').strip()
                    if not outf: outf = "export.xlsx"
                    export(rows, cols, outf)

                # feed the table name and field names to completer
                if qry_tp == util.TP_SHOW_TABLE or qry_tp == util.TP_DESC: nested_add_completer_word(rows)
        except KeyboardInterrupt: print()
        except BrokenPipeError:
            print("\nReconnecting...")
            ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)
            print("Reconnected, please try again")

    # disconnect websocket
    util.close_ws(ws)
    print("Bye!")


if __name__ == "__main__":
    launch_console()
