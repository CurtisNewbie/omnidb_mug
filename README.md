# omnidb_mug

Simple command line console tool for OmniDB.

```sh
python3 -m pip install -r requirements.txt

python3 src/main.py
```

- Omnidb >= 2.15: https://github.com/OmniDB/OmniDB/tree/2.15.0
- WebSocket Client Lib: https://github.com/websocket-client/websocket-client/
- [CurtisNewbie/excelparser](https://github.com/CurtisNewbie/excelparser) is needed for exporting query result to excel files


It now supports autocomplete for certain queries that don't specify the schema name they use. Use `USE {DATABASE_NAME}` to tell which database you want to use.

Then it will attempt to autocomplete following queries if possible.

- Very basic `SELECT ... FROM ...` queries, JOIN are not supported (that will be way too complex). 
- `SHOW TABLES ... LIKE ...` queries.
- `SHOW CREATE TABLE ...` queries.
- `DESC ...` queries.


## Demo

<img src="demo/demo1.jpeg" alt="demo1" width="600">
