import unittest
import main

class Tester(unittest.TestCase):

    def test_escape_sql(self):
        e = main.escape('select * from table where f = "12345";')
        assert e == 'select * from table where f = \\"12345\\";'

        e = main.escape('select * from table where f = \'12345\';')
        assert e == 'select * from table where f = \'12345\';'

    def test_change_instance(self):
        assert main.is_change_instance('\\change')
        assert main.is_change_instance('\\CHANGE')

    def test_is_reconnect(self):
        assert main.is_reconnect('\\reconnect')
        assert main.is_reconnect('\\RECONNECT')

    def test_is_export(self):
        assert main.is_export_cmd('\\export')
        assert main.is_export_cmd('\\EXPORT')

    def test_guess_qry_type(self):
        assert main.guess_qry_type('SELECT * FROM table where name = "gucci" limit 10') == main.TP_SELECT
        assert main.guess_qry_type('SELECT * FROM table where name in ("gucci", "juice") limit 10') == main.TP_SELECT
        assert main.guess_qry_type('SELECT * FROM table where f_name in ("gucci", "juice") limit 10') == main.TP_SELECT
        assert main.guess_qry_type('SELECT count(*) FROM table where name = "gucci" limit 10') == main.TP_SELECT
        assert main.guess_qry_type('SELECT count(*) FROM table;') == main.TP_SELECT
        assert main.guess_qry_type('SELECT name  FROM table;') == main.TP_SELECT
        assert main.guess_qry_type('SELECT * FROM apple.table where name = "gucci" limit 10') == main.TP_SELECT
        assert main.guess_qry_type('select * from .table where name = "gucci" limit 10') == main.TP_SELECT
        assert main.guess_qry_type('show tables in db like abc') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables in db') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables;') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables ;') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables like abc;') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables like abc ;') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables like \'abc\';') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('show tables like \'%ab_c\';') == main.TP_SHOW_TABLE
        assert main.guess_qry_type('desc my_table') == main.TP_DESC
        assert main.guess_qry_type('desc my_table ;') == main.TP_DESC
        assert main.guess_qry_type('desc my_table ;  ') == main.TP_DESC
        assert main.guess_qry_type('desc db.my_table ;') == main.TP_DESC
        assert main.guess_qry_type('desc db.my_table') == main.TP_DESC
        assert main.guess_qry_type('show create table db.abc ;') == main.TP_SHOW_CREATE_TABLE
        assert main.guess_qry_type('show create table db.abc') == main.TP_SHOW_CREATE_TABLE
        assert main.guess_qry_type('show create table abc ;') == main.TP_SHOW_CREATE_TABLE
        assert main.guess_qry_type('show create table abc ;   ') == main.TP_SHOW_CREATE_TABLE
        assert main.guess_qry_type('show create table abc;') == main.TP_SHOW_CREATE_TABLE
        assert main.guess_qry_type('use mydb') == main.TP_USE_DB
        assert main.guess_qry_type('use mydb  ;') == main.TP_USE_DB
        assert main.guess_qry_type('use  mydb  ') == main.TP_USE_DB
        assert main.guess_qry_type('use  mydb  ;') == main.TP_USE_DB


    def test_auto_complete_db(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'EXPLAIN SELECT * FROM table where name = "gucci" limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'EXPLAIN SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT * FROM table where name in ("gucci", "juice") limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name in ("gucci", "juice") limit 10'

        sql = 'SELECT * FROM table where f_name in ("gucci", "juice") limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where f_name in ("gucci", "juice") limit 10'

        sql = 'SELECT * FROM table left join another_table on ta = tb'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table left join my_db.another_table on ta = tb'

        sql = 'SELECT * FROM table right join another_table on ta = tb'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table right join my_db.another_table on ta = tb'

        sql = 'SELECT * FROM table left join another_table   using (join_col)  right  join  yet_ano_table on ta = tb where name in ("gucci", "juice") limit 10 '
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table left join my_db.another_table   using (join_col)  right  join  my_db.yet_ano_table on ta = tb where name in ("gucci", "juice") limit 10'

        sql = 'SELECT * FROM table left join another_table   using (join_col)  right  join  yet_ano_table on ta = tb where name in ("gucci", "juice") limit 10 '
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table left join my_db.another_table   using (join_col)  right  join  my_db.yet_ano_table on ta = tb where name in ("gucci", "juice") limit 10'

        sql = 'SELECT count(*) FROM table where name = "gucci" limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT count(*) FROM table;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table;'

        sql = 'SELECT name, desc FROM table;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT name, desc FROM my_db.table;'

        sql = 'SELECT name  FROM table;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT name  FROM my_db.table;'

        sql = 'SELECT count(*) FROM table ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table ;'

        sql = 'SELECT * FROM apple.table where name = "gucci" limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'select * from .table where name = "gucci" limit 10'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'show tables in db like abc'
        completed = main.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'show tables in db'
        completed = main.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'show tables'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db'

        sql = 'show tables;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables like abc;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc;'

        sql = 'show tables like abc ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc ;'

        sql = 'show tables like \'abc\';'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'abc\';'

        sql = 'show tables like \'%ab_c\';'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'%ab_c\';'

        sql = 'desc my_table'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table'

        sql = 'desc my_table ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc my_table ;  '
        completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc db.my_table ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table ;'

        sql = 'desc db.my_table'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table'

        sql = 'show create table db.abc ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc ;'

        sql = 'show create table db.abc'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc'

        sql = 'show create table abc ;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc ;   '
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc;'
        completed = main.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc;'


    def test_parse_pretty_print(self):
        p, s = main.parse_pretty_print("select 1 from abc \G")
        assert p
        assert s == "select 1 from abc"

        p, s = main.parse_pretty_print("select 1 from abc \g")
        assert p
        assert s == "select 1 from abc"

        p, s = main.parse_pretty_print("select 1 from abc \G;")
        assert p
        assert s == "select 1 from abc ;"

        p, s = main.parse_pretty_print("select 1 from abc \G ;")
        assert p
        assert s == "select 1 from abc  ;"


    def test_is_exit_cmd(self):
        assert main.is_exit("exit")
        assert not main.is_exit("exit(")
        assert main.is_exit("exit()")
        assert main.is_exit("quit")
        assert main.is_exit("quit()")
        assert main.is_exit("QUIT()")
        assert main.is_exit("\quit")
        assert main.is_exit("\exit")


    def test_extract_schema_table(self):
        sql = 'SELECT * FROM mydb.table where name = "gucci" limit 10'
        schema, table = main.extract_schema_table(sql)
        assert schema == 'mydb'
        assert table == 'table'

        sql = 'SELECT * FROM mydb.table'
        schema, table = main.extract_schema_table(sql)
        assert schema == 'mydb'
        assert table == 'table'


if __name__ == "__main__":
  unittest.main()
