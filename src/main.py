import util
import getpass

csrf = 'NqZm7ziofRZchMRZfrg9LXaQ1TdJWVcqn9XdSpKi8GsJZUlmeZTN8DNzY7ZjmAdl'
uname = ''
pw = ''
host = ""

if __name__ == "__main__":
    if not uname:
        uname = input("Enter Username: ")
    if not pw:
        pw = getpass.getpass(f"Enter Password for '{uname}':")

    # login
    sh = util.login(csrf, host, uname, pw)
    print(f"cookie: {sh.cookie}")

    # connect websocket
    ws = util.connect_ws(sh, host)

    # change active database
    # TODO remove hard-coded values
    util.change_active_database(sh, 1, "conn_tabs_tab4", "")

    # first message
    # TODO remove hard-coded values
    util.send_recv(ws, f'{{"v_code":0,"v_context_code":0,"v_error":false,"v_data":"{sh.sessionid}"}}')

    # execute queries
    # TODO remove hard-coded values
    print("Entering sql query interactive mode, enter 'quit()' to exit")
    while True: 
        sql = input("Enter Query: ").strip()
        if sql == 'quit()': break
        if sql == "": continue

        msg = f'{{"v_code":1,"v_context_code":3,"v_error":false,"v_data":{{"v_sql_cmd":"{sql}","v_sql_save":"{sql}","v_cmd_type":null,"v_db_index":1,"v_conn_tab_id":"conn_tabs_tab4","v_tab_id":"conn_tabs_tab4_tabs_tab1","v_tab_db_id":57,"v_mode":0,"v_all_data":false,"v_log_query":true,"v_tab_title":"Query","v_autocommit":true}}}}'
        util.send_recv(ws, msg, 2)

    # disconnect websocket
    ws.close()
    print("Websocket disconnected")