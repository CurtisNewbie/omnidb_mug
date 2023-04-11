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

# username, env: OMNIDB_MUG_USER
uname = ''

# http protocol
http_protocol = util.DEFAULT_HTTP_PROTOCOL 

# websocket protocol
ws_protocol = util.DEFAULT_WS_PROTOCOL 

# host (without protocol), env: OMNIDB_MUG_HOST
host = ''

# debug mode, env: OMNIDB_MUG_DEBUG
debug = False       

# force to use OFFSET,LIMIT to export, env: OMNIDB_MUG_FORCE_BATCH_EXPORT
force_batch_export = False

# disable auto complete, env: OMNIDB_MUG_DISABLE_AUTO_COMPLETE
disable_auto_complete = False

# page limit of batch export
batch_export_limit = 400  

# sleep time in ms for each batch export
batch_export_throttle_ms = 200

use_database: str = ""


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
def parse_use_db(sql: str) -> bool:
    global use_database 
    m = use_db_sql_pat.match(sql)
    if m: 
        use_database = m.group(1).strip()
        print(f"Using databse: '{use_database}'")
        return True
    return False


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


def launch_console():
    global debug, v_tab_id, v_conn_tab_id, uname, host, batch_export_limit, http_protocol, ws_protocol, use_database, force_batch_export

    ap = argparse.ArgumentParser(description="OmniDB Mug", formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('-p', '--password', type=str, help=f"Password", default="")
    args = ap.parse_args()
    
    ws: WebSocket = None

    disable_db_auto_complete = False
    if os.getenv('OMNIDB_MUG_DISABLE_AUTO_COMPLETE') and os.getenv('OMNIDB_MUG_DISABLE_AUTO_COMPLETE').lower() == 'true': disable_db_auto_complete = True
    if os.getenv('OMNIDB_MUG_FORCE_BATCH_EXPORT') and os.getenv('OMNIDB_MUG_FORCE_BATCH_EXPORT').lower() == 'true': force_batch_export = True
    if os.getenv('OMNIDB_MUG_DEBUG') and os.getenv('OMNIDB_MUG_DEBUG').lower() == 'true': debug = True

    util.env_print("Using HTTP Protocol", http_protocol)
    util.env_print("Using WebSocket Protocol", ws_protocol)
    util.env_print("Force Batch Export (OFFSET, LIMIT)", force_batch_export)
    util.env_print("Debug Mode", debug)
    util.env_print("Disable DB Auto-Complete", disable_db_auto_complete)

    qry_ctx = util.QueryContext()
    qry_ctx.v_conn_tab_id = v_conn_tab_id
    qry_ctx.v_tab_id = v_tab_id
    
    try:
        if not host: host = os.getenv('OMNIDB_MUG_HOST')
        while not host: input("Enter host of Omnidb: ")
        util.env_print("Using Host", host)

        if not uname: uname = os.getenv('OMNIDB_MUG_USER')
        while not uname: uname = input("Enter Username: ")
        util.env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = util.get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        if args.password: pw = args.password 
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

    # execute queries
    print()
    print("Switching to interactive mode, enter 'quit()' or 'quit' or 'exit' to exit")
    print("Enter 'export [SQL]' to export excel files (csv/xlsx/xls)")
    print("Enter 'change instance' to change the connected instance")
    print()

    qry_ctx.v_context_code = 2 # start with two, when we connect websocket, we always send the first msg to server right away
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
                if force_batch_export: batch_export = True
                elif not util.query_has_limit(sql):
                    batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'
            
            if is_change_instance(cmd):
                qry_ctx = select_instance(sh, qry_ctx, debug=debug) 
                continue

            if debug: print(f"sql: '{sql}'")
            auto_comp_db = False
            if not disable_db_auto_complete: auto_comp_db, sql = util.auto_complete_db(sql, use_database)
            if not auto_comp_db and parse_use_db(sql): continue

            if debug: print(f"preprocessed sql: '{sql}'")

            if batch_export:
                outf = input('Please specify where to export (default to \'batch_export.xlsx\'): ').strip()
                if not outf: outf = "batch_export.xlsx"

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
        except KeyboardInterrupt:
            print()

        except BrokenPipeError:
            print("\nReconnecting...")
            ws = util.ws_connect(sh, host, debug=debug, protocol=ws_protocol)

    # disconnect websocket
    util.close_ws(ws)
    print("Bye!")


if __name__ == "__main__":
    launch_console()
