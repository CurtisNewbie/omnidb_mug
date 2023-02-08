# omnidb_mug

Omnidb Repo: https://github.com/OmniDB/OmniDB/tree/2.15.0
Compatibility: omnidb v2.15.0
Client Lib: https://github.com/websocket-client/websocket-client/

Omnidb source code:

- for websocket, `ws_core.py:622`
- for command dispatching `ws_core.py:155`

## Database Info 

POST `.../get_database_list/`

- `cookie: omnidb_csrftoken=xxxxxxxxxxxxxxxxxx; omnidb_sessionid=xxxxxxxxxxxxxxxxxxxx`

Response:

```json
{
    "v_data": {
        "v_select_html": "...",
        "v_select_group_html": "...",
        "v_connections": [
            {
                "v_db_type": "mysql",
                "v_alias": "[name 1]",
                "v_conn_id": 1,
                "v_console_help": "...",
                "v_database": ""
            },
            {
                "v_db_type": "mysql",
                "v_alias": "[name 2]",
                "v_conn_id": 2,
                "v_console_help": "...",
                "v_database": ""
            }
        ],
        "v_groups": [
            {
                "v_group_id": 0,
                "v_name": "All connections",
                "conn_list": []
            }
        ],
        "v_remote_terminals": [],
        "v_id": 0,
        "v_existing_tabs": [
            {
                "index": 1,
                "snippet": "",
                "title": "Query",
                "tab_db_id": 57
            }
        ]
    },
    "v_error": false,
    "v_error_id": -1
}
```


## Flow

T0: client -> server (`v_data` is the `omnidb_sessionid` in cookie)

```json
{"v_code":0,"v_context_code":0,"v_error":false,"v_data":"xxxxxxxwd4lvi5shpujfl8d62lce779a"}
```

T1: client -> server (query)

```json
{
    "v_code": 1,
    "v_context_code": 2,
    "v_error": false,
    "v_data": {
        "v_sql_cmd": "[SQL QUERY]",
        "v_sql_save": "",
        "v_cmd_type": null,
        "v_db_index": 1,
        "v_conn_tab_id": "conn_tabs_tab4",
        "v_tab_id": "conn_tabs_tab4_tabs_tab1",
        "v_tab_db_id": 57,
        "v_mode": 0,
        "v_all_data": false,
        "v_log_query": true,
        "v_tab_title": "Query",
        "v_autocommit": true
    }
}
```

T2: server -> client (result)

```json
{
    "v_code": 1,
    "v_context_code": 2,
    "v_error": false,
    "v_data": {
        "v_col_names": [
            "col1",
            "col2"
        ],
        "v_data": [
            [
                "r1data1",
                "r1data2"
            ],
            [
                "r2data1",
                "r2data2"
            ]
        ],
        "v_last_block": true,
        "v_duration": "69.235 ms",
        "v_notices": "",
        "v_notices_length": 0,
        "v_inserted_id": null,
        "v_status": 18,
        "v_con_status": 1,
        "v_chunks": true
    }
}
```




