import util
import getpass
import os
import re
import readline # don't remove this, this is for input()

EXPORT_LEN = len("export")

# TODO these doesn't seem to be important :D
v_tab_id="conn_tabs_tab4_tabs_tab1"
v_conn_tab_id = "conn_tabs_tab4"

# Credentials, Host
# environment variable: OMNIDB_MUG_HOST
# environment variable: OMNIDB_MUG_USER
uname = ''
host = ''

# configuration
# environment variable: OMNIDB_MUG_DEBUG
debug = False       
export_limit = 400  


def export(cols, rows, outf):
    # https://github.com/CurtisNewbie/excelparser
    import excelparser
    ep = excelparser.ExcelParser(outf)   
    ep.rows = rows
    ep.cols = cols 
    ep.export(outf)
    print(f"Exported to '{outf}'")


def is_export_cmd(cmd: str) -> str:
    return re.match("[Ee][Xx][Pp][Oo][Rr][Tt].*" , cmd)  # cmd is trimmed already 


def launch_console():
    global debug, v_tab_id, v_conn_tab_id, uname, host, export_limit

    if os.getenv('OMNIDB_MUG_DEBUG') and os.getenv('OMNIDB_MUG_DEBUG').lower() == 'true': debug = True

    if not host: host = os.getenv('OMNIDB_MUG_HOST')
    if not host: input("Enter host of Omnidb: ")
    if not host: util.sys_exit(0, "Host of Omnidb is required")

    if not uname: uname = os.getenv('OMNIDB_MUG_USER')
    if not uname: uname = input("Enter Username: ")
    if not uname: util.sys_exit(0, "Username is required")

    # retrieve csrf token first by request '/' path
    csrf = util.get_csrf_token(host, debug=debug)

    pw = getpass.getpass(f"Enter Password for '{uname}':").strip()
    if not pw: util.sys_exit(0, "Password is required")

    # login
    sh = util.login(csrf, host, uname, pw, debug=debug)

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

    selected_db = input("Please select database: ") 
    if not selected_db: selected_db = prev_selected 
    selected_db = int(selected_db)
    v_db_index = db.connections[selected_db].v_conn_id
    print(f'Selected database \'{db.connections[selected_db].v_alias}\'')

    # change active database
    util.change_active_database(sh, v_db_index, v_conn_tab_id, "", debug=debug)

    # connect websocket
    ws = util.ws_connect(sh, host, debug=debug)

    # first message
    util.ws_send_recv(ws, f'{{"v_code":0,"v_context_code":0,"v_error":false,"v_data":"{sh.sessionid}"}}', log_msg=debug)

    # execute queries
    print("Switching to interactive mode, enter 'quit()' or 'quit' or 'exit' to exit")
    print("Enter 'export [SQL]' to export excel files (csv/xlsx/xls)")
    ctx_id = 2
    while True: 
        try:
            cmd = input("> ").strip().lower()
            if cmd == 'quit()' or cmd == 'quit' or cmd == 'exit': break
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

            if debug: print(f"sql: '{sql}'")

            # TODO: this part of the code looks so stupid, but it kinda works, fix it later :D
            if batch_export:
                outf = input('Please specify where to export (default to \'batch_export.xlsx\'): ').strip()
                if not outf: outf = "batch_export.xlsx"
                offset = 0  
                acc_cols = []
                acc_rows = []

                while True: 
                    if sql.endswith(";"): sql = sql[:len(sql) - 1]
                    offset_sql = sql + f" limit {offset}, {export_limit}" 
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
                    if len(rows) < 1 or len(rows) < export_limit: break
                    offset += export_limit

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
    ws.close()
    if debug: print("Websocket disconnected")
    print("Bye!")


if __name__ == "__main__":
    launch_console()
