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

# page limit of batch export
batch_export_limit = 400  

# sleep time in ms for each batch export
batch_export_throttle_ms = 200

use_database: str = ""


def select_instance(sh: util.OSession, select_first = False, debug = False) -> tuple[str, str]:
    # list database, pick one to use
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
    return v_tab_db_id, v_db_index


use_db_sql_pat =  re.compile("[Uu][Ss][Ee] *([0-9a-zA-Z_]*) *;?") 
def parse_use_db(sql: str) -> bool:
    global use_database 
    m = use_db_sql_pat.match(sql)
    if m: 
        use_database = m.group(1).strip()
        print(f"Using databse: '{use_database}'")
        return True
    return False

slt_sql_pat =  re.compile("[Ss][Ee][Ll][Ee][Cc][Tt].*") 
def is_select(sql: str) -> bool:
    return slt_sql_pat.match(sql)


slt_sql_pat = re.compile("^select *[0-9a-zA-Z_\*]* *from *(\.?\w+)( *| \w+ ?.*)$", re.IGNORECASE) 
show_tb_pat = re.compile("^show *tables( *);? *$", re.IGNORECASE) 
desc_tb_pat = re.compile("^desc *(.?[0-9a-zA-Z_]+) *;?$", re.IGNORECASE) 
def complete_database(sql: str, database: str) -> tuple[bool, str]:
    start = time.monotonic_ns()
    
    if not database: return False, sql
    sql = sql.strip()

    completed = False
    m = slt_sql_pat.match(sql)
    if m: 
        open, close = m.span(1)
        sql = insert_db_name(sql, database, open, close) 
        completed = True

    if not completed:
        m = show_tb_pat.match(sql)
        if m:
            open, close = m.span(1)
            sql = sql[: open] + f" in {database}" + sql[close:]
            completed = True

    if not completed:
        m = desc_tb_pat.match(sql)
        if m:
            open, close = m.span(2)
            sql = insert_db_name(sql, database, open, close) 
            completed = True

    if completed: print(f"Auto-completed ({(time.monotonic_ns() - start) / 1000}ms): {sql}")
    return completed, sql


def insert_db_name(sql: str, database: str, open: int, close: int) -> str:
    table = sql[open:close].strip()
    l = table.find(".")
    if l < 0: table = "." + table
    table = database + table 
    sql = sql[: open] + table + sql[close:]
    return sql


def env_print(key, value):
    prop = key + ":"
    print(f"{prop:40}{value}")


def is_exit(cmd: str) -> bool:
    return cmd == 'quit()' or cmd == 'quit' or cmd == 'exit'


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
    global debug, v_tab_id, v_conn_tab_id, uname, host, batch_export_limit, http_protocol, ws_protocol, use_database
    ws: WebSocket = None

    if os.getenv('OMNIDB_MUG_DEBUG') and os.getenv('OMNIDB_MUG_DEBUG').lower() == 'true': debug = True
    env_print("Using HTTP Protocol", http_protocol)
    env_print("Using WebSocket Protocol", ws_protocol)
    
    try:
        if not host: host = os.getenv('OMNIDB_MUG_HOST')
        while not host: input("Enter host of Omnidb: ")
        env_print("Using Host", host)

        if not uname: uname = os.getenv('OMNIDB_MUG_USER')
        while not uname: uname = input("Enter Username: ")
        env_print("Using Username", uname)
        print()

        # retrieve csrf token first by request '/' path
        csrf = util.get_csrf_token(host, protocol=http_protocol, debug=debug)

        pw = ""
        while not pw: pw = getpass.getpass(f"Enter Password for '{uname}': ").strip()

        # login
        sh = util.login(csrf, host, uname, pw, protocol=http_protocol, debug=debug)

        # list database, pick one to use
        v_tab_db_id, v_db_index = select_instance(sh, select_first=True, debug=debug) 

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
    ctx_id = 2
    while True: 
        try:
            cmd = input(f"({use_database}) > " if use_database else "> ").strip().lower()
            if is_exit(cmd): break
            if cmd == "": continue
            ctx_id += 1

            batch_export = False
            sql = cmd
            
            do_export = is_export_cmd(cmd)
            if do_export: 
                sql: str = sql[EXPORT_LEN:].strip()
                if sql == "": continue
                if not util.query_has_limit(sql):
                    batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'
            
            if is_change_instance(cmd):
                # list database, pick one to use
                v_tab_db_id, v_db_index = select_instance(sh, debug=debug) 
                continue

            if debug: print(f"sql: '{sql}'")
            autocomp, sql = complete_database(sql, use_database)
            if not autocomp: 
                if parse_use_db(sql): continue

            if debug: print(f"preprocessed sql: '{sql}'")

            # TODO: this part of the code looks so stupid, but it kinda works, fix it later :D
            if batch_export:
                outf = input('Please specify where to export (default to \'batch_export.xlsx\'): ').strip()
                if not outf: outf = "batch_export.xlsx"
                offset = 0  
                acc_cols = []
                acc_rows = []

                while True: 
                    if sql.endswith(";"): sql = sql[:len(sql) - 1]
                    offset_sql = sql + f" limit {offset}, {batch_export_limit}" 
                    print(offset_sql)
                    ok, cols, rows = util.exec_query(ws, offset_sql, 
                                    log_msg=debug,
                                    v_db_index=v_db_index, 
                                    v_context_code=ctx_id,
                                    v_conn_tab_id=v_conn_tab_id,
                                    v_tab_id=v_tab_id, 
                                    v_tab_db_id=v_tab_db_id)
                    if not ok: break 
                    if offset < 1: acc_cols = cols
                    acc_rows += rows
                    if len(rows) < 1 or len(rows) < batch_export_limit: break
                    offset += batch_export_limit
                    if batch_export_throttle_ms > 0: time.sleep(batch_export_throttle_ms / 1000)

                export(acc_rows, acc_cols, outf)
            else:
                ok, cols, rows = util.exec_query(ws, sql, 
                                log_msg=debug,
                                v_db_index=v_db_index, 
                                v_context_code=ctx_id,
                                v_conn_tab_id=v_conn_tab_id,
                                v_tab_id=v_tab_id, 
                                v_tab_db_id=v_tab_db_id)
                if not ok: continue
                if do_export: 
                    outf = input('Please specify where to export (default to \'export.xlsx\'): ').strip()
                    if not outf: outf = "export.xlsx"
                    export(rows, cols, outf)
        except KeyboardInterrupt:
            print()

    # disconnect websocket
    util.close_ws(ws)
    print("Bye!")


if __name__ == "__main__":
    launch_console()
