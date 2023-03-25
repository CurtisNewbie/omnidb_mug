import util
import getpass
import sys
import os
import re
import readline # don't remove this, this is for input()

EXPORT_LEN = len("export")

# TODO remove hard-coded values
# Session based stuff, we can use Chrome dev tool to check the values that we may use
v_tab_db_id=57
v_tab_id="conn_tabs_tab4_tabs_tab1"
v_db_index=1
v_conn_tab_id = "conn_tabs_tab4"
csrf = 'NqZm7ziofRZchMRZfrg9LXaQ1TdJWVcqn9XdSpKi8GsJZUlmeZTN8DNzY7ZjmAdl'

# Credentials, Host
# environment variable: OMNIDB_MUG_HOST
# environment variable: OMNIDB_MUG_USER
uname = ''
host = ''

# configuration
debug=False
export_limit = 400


def is_export(cmd: str) -> str:
    return re.match("[Ee][Xx][Pp][Oo][Rr][Tt].*" , cmd)  # cmd is trimmed already 


def launch_console():
    global v_tab_db_id
    global v_tab_id
    global v_db_index
    global v_conn_tab_id 
    global csrf 
    global uname
    global host
    global export_limit

    if not host: host = os.getenv('OMNIDB_MUG_HOST')
    if not host:
        print(f"Please specify host of Omnidb")
        sys.exit(0)

    if not uname: uname = os.getenv('OMNIDB_MUG_USER')
    if not uname: uname = input("Enter Username: ")

    pw = getpass.getpass(f"Enter Password for '{uname}':").strip()
    if not pw:
        print(f"Please enter Password for '{uname}':")
        sys.exit(0)

    # login
    sh = util.login(csrf, host, uname, pw)

    # change active database
    util.change_active_database(sh, v_db_index, v_conn_tab_id, "")

    # connect websocket
    ws = util.ws_connect(sh, host)

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
            export = is_export(cmd)
            if export: 
                sql: str = sql[EXPORT_LEN:].strip()
                if sql == "": continue
                if not util.query_has_limit(sql) :
                    batch_export = input('Batch export using offset/limit? [y/n] ').strip().lower() == 'y'

            if debug: print(f"sql: '{sql}'")

            # TODO: this part of the code looks so stupid, but it kinda works, fix it later :D
            if batch_export:
                outf = input('Please specify where to export: ').strip()
                if not outf: outf = "batch_export.xlsx"
                offset = 0  
                acc_cols = []
                acc_rows = []

                while True: 
                    if sql.endswith(";"): sql = sql[:len(sql) - 1]
                    offset_sql = sql + f" limit {offset}, {export_limit}" 
                    print(offset_sql)
                    cols, rows = util.exec_query(ws, offset_sql, 
                                    log_msg=debug,
                                    v_db_index=v_db_index, 
                                    v_context_code=ctx_id,
                                    v_conn_tab_id=v_conn_tab_id,
                                    v_tab_id=v_tab_id, 
                                    v_tab_db_id=v_tab_db_id)
                
                    if offset < 1: acc_cols = cols
                    acc_rows += rows
                    if len(rows) < 1 or len(rows) < export_limit: break
                    offset += export_limit

                # https://github.com/CurtisNewbie/excelparser
                import excelparser
                ep = excelparser.ExcelParser(outf)   
                ep.rows = acc_rows
                ep.cols = acc_cols 
                ep.export(outf)
                print(f"Exported to '{outf}'")
            else:
                cols, rows = util.exec_query(ws, sql, 
                                log_msg=debug,
                                v_db_index=v_db_index, 
                                v_context_code=ctx_id,
                                v_conn_tab_id=v_conn_tab_id,
                                v_tab_id=v_tab_id, 
                                v_tab_db_id=v_tab_db_id)

                # https://github.com/CurtisNewbie/excelparser
                if export: 
                    outf = input('Please specify where to export: ').strip()
                    if not outf: outf = "export.xlsx"
                    import excelparser
                    ep = excelparser.ExcelParser(outf)   
                    ep.rows = rows
                    ep.cols = cols 
                    ep.export(outf)
                    print(f"Exported to '{outf}'")
        except KeyboardInterrupt:
            print("\nEnter 'exit' to exit")

    # disconnect websocket
    ws.close()
    print("Websocket disconnected")


if __name__ == "__main__":
    launch_console()
