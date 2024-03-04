# omnidb_mug

CLI console for OmniDB in under 1000 lines of code :D (the code is terrible but it works)

```sh
# install dependencies
python3 -m pip install websocket-client requests

# install dependencies using a specific source address
python3 -m pip install websocket-client requests \
    -i https://pypi.tuna.tsinghua.edu.cn/simple

# start omnidb_mug with interactive mode
python3 src/main.py

# start omnidb_mug with scripting mode
python3 src/main.py --script myscript.sql

# show help doc for omnidb_mug
python3 src/main.py -h
```

- Omnidb >= 2.15: https://github.com/OmniDB/OmniDB/tree/2.15.0
- WebSocket Client Lib: https://github.com/websocket-client/websocket-client/
- [CurtisNewbie/excelparser](https://github.com/CurtisNewbie/excelparser) is needed for exporting query result to excel files

omnidb_mug supports tab completion, everytime you execute `SHOW TABLES`, `DESC ...` or even `USE ...`, omnidb_mug feeds the data to the word completer.

omnidb_mug also supports schema name completion. You have to execute `'USE {SCHEMA_NAME}'` to tell which database you want to use, it's not supported by omnidb, but omnidb_mug recognizes the query, and will automatically complete the schema name for you, it only works for relatively simple queries though. For rather complex queries, you will still need to include the schema name in your queries just in case that the completer is not working properly.

For example, tell omnidb_mug that we are using schema 'mydb'

```bash
(omnidb) > use mydb
Fetching tables names in 'mydb' for auto-completion
```

Then the schema name completion should work as the following, (I personally expect that) it should cover most of the need:

```sql
SELECT * FROM my_table WHERE name = "123" ORDER BY id DESC LIMIT 10
-- SELECT * FROM my_db.my_table WHERE name = "123" ORDER BY id DESC LIMIT 10

SELECT * FROM my_table_1 t1
LEFT JOIN my_table_2 t2 ON t1.id = t2.id
LEFT JOIN my_table_3 t3 USING (od_no)
WHERE t1.name = "123" ORDER BY t1.id DESC LIMIT 10
--  SELECT * FROM my_db.my_table_1 t1
--  LEFT JOIN my_db.my_table_2 t2 ON t1.id = t2.id
--  LEFT JOIN my_db.my_table_3 t3 USING (od_no)
--  WHERE t1.name = "123" ORDER BY t1.id DESC LIMIT 10

DESC my_table
-- DESC my_db.my_table

SHOW TABLES
-- SHOW TABLES in my_db

SHOW CREATE TABLE my_table
-- SHOW CREATE TABLE my_db.my_table
```

**Pretty Print** can also be enabled by appending the `\G` (case-insensitive) flag at the end of the query. For example:

```sql
select * from my_database.my_table limit 1 \G
```

More **Special Commands**:

- `\reconnect` to reconnect the websocket
- `\change` to change the instance used
- `\export [SQL]` to export the result of query as an excel file
- `\debug` to enable/disable debug mode (only works in interactive session)
- `\insert` to dump INSERT sql for the returned rows

## Scripting

Scripting is also supported with the `--script YOUR_SCRIPTING_FILE`. The file should contain a bunch of SQLs that you want to execute one by one.

For example:

```sql
SELECT * FROM mydb.mytable WHERE id = 1;
SELECT * FROM mydb.mytable WHERE id = 2;
SELECT * FROM mydb.mytable WHERE id = 3;
```

## Demo

Copied from terminal:

```bash
Python Version:                         3.10.13 (main, Nov  1 2023, 17:29:04) [Clang 14.0.3 (clang-1403.0.22.14.1)]
Omnidb_Mug Version:                     0.0.12
Using HTTP Protocol:                    https://
Using WebSocket Protocol:               wss://
Force Batch Export (OFFSET, LIMIT):     True
Log File:                               *************/omnidb_mug/exec.log
AutoCompleter Cache:                    *************/omnidb_mug/cache.json
Excluded Columns for INSERT Dump:       id
Multi-line Console Input:               True
Using Host:                             **********************
Using Username:                         **********************

Available database connections:
  [0] '********'
  [1] '**********'
* [2] '*********'
Selected database '*********'

Switching to interactive mode, type 'quit' to exit

 \export SELECT_SQL    export results as an excel file
 \insert SELECT_SQL    generate INSERT SQL
 \change               change the connected instance
 \reconnect            reconnect the websocket connection
 \debug                enable/disable debug mode

(*********) > select 1;

|----|
| 1  |
|----|
| 1  |
|----|

Total    : 1
Cost     : 69.491 ms
Wall Time: 446.52 ms
```
