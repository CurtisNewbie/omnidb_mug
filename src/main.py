import util
import getpass
import sys
import os

CMD_QUERY  = "query:"
CMD_EXPORT = "export:"

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


def extract_sql(cmd: str) -> str:
    if cmd.startswith(CMD_EXPORT): return cmd[len(CMD_EXPORT):].strip()
    if cmd.startswith(CMD_QUERY): return cmd[len(CMD_QUERY):].strip()
    return cmd


def launch_console():
    global v_tab_db_id
    global v_tab_id
    global v_db_index
    global v_conn_tab_id 
    global csrf 
    global uname
    global host

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
    print("Entering sql query interactive mode, enter 'quit()' or 'quit' or 'exit' to exit")
    ctx_id = 2
    while True: 
        try:
            cmd = input("> ").strip().lower()
            if cmd == 'quit()' or cmd == 'quit' or cmd == 'exit': break
            if cmd == "": continue
            ctx_id += 1

            sql = extract_sql(cmd)
            if sql == "": continue

            cols, rows = util.exec_query(ws, sql, 
                            log_msg=False,
                            v_db_index=v_db_index, 
                            v_context_code=ctx_id,
                            v_conn_tab_id=v_conn_tab_id,
                            v_tab_id=v_tab_id, 
                            v_tab_db_id=v_tab_db_id)

            # https://github.com/CurtisNewbie/excelparser
            if cmd.startswith(CMD_EXPORT): 
                import excelparser
                ep = excelparser.ExcelParser()   
                ep.rows = rows
                ep.cols = cols 
                outf = input('Please specify where to export: ').strip()
                if outf: 
                    print(f"Exporting to '{outf}'")
                    ep.export(outf)
        except KeyboardInterrupt:
            print("\nEnter 'exit' to exit")

    # disconnect websocket
    ws.close()
    print("Websocket disconnected")


if __name__ == "__main__":
    launch_console()
