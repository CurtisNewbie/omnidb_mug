# omnidb_mug

Simple command line console tool for OmniDB.

```sh
python3 -m pip install -r requirements.txt

python3 src/main.py
```

- Omnidb >= 2.15: https://github.com/OmniDB/OmniDB/tree/2.15.0
- WebSocket Client Lib: https://github.com/websocket-client/websocket-client/
- [CurtisNewbie/excelparser](https://github.com/CurtisNewbie/excelparser) is needed for exporting query result to excel files


It now supports auto-completion for database, table and field (by typing TAB). Query `'USE {DATABASE_NAME}'` to tell which database you want to use, you will still need to include the schema name in your queries because OmniDB doesn't support this.

Pretty print can be enabled by appending the `\G` (case-insensitive) flag at the end of the query. For example:

More **Special Commands**:

- `\reconnect` to reconnect the websocket
- `\change` to change the instance used
- `\export [SQL]` to export the result of query as an excel file

```sql
select * from my_table limit 1 \G
```

## Demo

<img src="demo/demo1.jpeg" alt="demo1" width="600">
