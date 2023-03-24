import util
import getpass

v_tab_db_id=57
v_tab_id="conn_tabs_tab4_tabs_tab1"
v_db_index=1
v_conn_tab_id = "conn_tabs_tab4"
csrf = 'NqZm7ziofRZchMRZfrg9LXaQ1TdJWVcqn9XdSpKi8GsJZUlmeZTN8DNzY7ZjmAdl'

uname = ''
pw = ''
host = ''

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
    util.change_active_database(sh, v_db_index, v_conn_tab_id, "")

    # first message
    util.send_recv(ws, f'{{"v_code":0,"v_context_code":0,"v_error":false,"v_data":"{sh.sessionid}"}}')

    # execute queries
    # TODO remove hard-coded values
    print("Entering sql query interactive mode, enter 'quit()' to exit")
    ctx_id = 3
    while True: 
        sql = input("Enter Query: ").strip()
        if sql == 'quit()' or sql == 'quit' or sql == 'exit': break
        if sql == "": continue
        util.exec_query(ws, sql, v_db_index=v_db_index, 
                        v_context_code=ctx_id,
                        v_conn_tab_id=v_conn_tab_id,
                        v_tab_id=v_tab_id, 
                        v_tab_db_id=v_tab_db_id)
        ctx_id += 1

    # disconnect websocket
    ws.close()
    print("Websocket disconnected")