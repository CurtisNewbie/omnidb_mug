import unittest
import util

class Tester(unittest.TestCase):

    def test_escape_sql(self):
        e = util.escape('select * from table where f = "12345";')
        assert e == 'select * from table where f = \\"12345\\";'

        e = util.escape('select * from table where f = \'12345\';')
        assert e == 'select * from table where f = \'12345\';'


    def test_guess_qry_type(self):
        assert util.guess_qry_type('SELECT * FROM table where name = "gucci" limit 10') == util.TP_SELECT
        assert util.guess_qry_type('SELECT * FROM table where name in ("gucci", "juice") limit 10') == util.TP_SELECT
        assert util.guess_qry_type('SELECT * FROM table where f_name in ("gucci", "juice") limit 10') == util.TP_SELECT
        assert util.guess_qry_type('SELECT count(*) FROM table where name = "gucci" limit 10') == util.TP_SELECT
        assert util.guess_qry_type('SELECT count(*) FROM table;') == util.TP_SELECT
        assert util.guess_qry_type('SELECT name  FROM table;') == util.TP_SELECT
        assert util.guess_qry_type('SELECT * FROM apple.table where name = "gucci" limit 10') == util.TP_SELECT
        assert util.guess_qry_type('select * from .table where name = "gucci" limit 10') == util.TP_SELECT
        assert util.guess_qry_type('show tables in db like abc') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables in db') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables;') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables ;') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables like abc;') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables like abc ;') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables like \'abc\';') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('show tables like \'%ab_c\';') == util.TP_SHOW_TABLE
        assert util.guess_qry_type('desc my_table') == util.TP_DESC
        assert util.guess_qry_type('desc my_table ;') == util.TP_DESC
        assert util.guess_qry_type('desc my_table ;  ') == util.TP_DESC
        assert util.guess_qry_type('desc db.my_table ;') == util.TP_DESC
        assert util.guess_qry_type('desc db.my_table') == util.TP_DESC
        assert util.guess_qry_type('show create table db.abc ;') == util.TP_SHOW_CREATE_TABLE
        assert util.guess_qry_type('show create table db.abc') == util.TP_SHOW_CREATE_TABLE
        assert util.guess_qry_type('show create table abc ;') == util.TP_SHOW_CREATE_TABLE
        assert util.guess_qry_type('show create table abc ;   ') == util.TP_SHOW_CREATE_TABLE
        assert util.guess_qry_type('show create table abc;') == util.TP_SHOW_CREATE_TABLE
        assert util.guess_qry_type('use mydb') == util.TP_USE_DB
        assert util.guess_qry_type('use mydb  ;') == util.TP_USE_DB
        assert util.guess_qry_type('use  mydb  ') == util.TP_USE_DB
        assert util.guess_qry_type('use  mydb  ;') == util.TP_USE_DB


    def test_auto_complete_db(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT * FROM table where name in ("gucci", "juice") limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name in ("gucci", "juice") limit 10'

        sql = 'SELECT * FROM table where f_name in ("gucci", "juice") limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where f_name in ("gucci", "juice") limit 10'

        sql = 'SELECT count(*) FROM table where name = "gucci" limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT count(*) FROM table;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table;'

        sql = 'SELECT name, desc FROM table;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT name, desc FROM my_db.table;'

        sql = 'SELECT name  FROM table;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT name  FROM my_db.table;'

        sql = 'SELECT count(*) FROM table ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table ;'

        sql = 'SELECT * FROM apple.table where name = "gucci" limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'select * from .table where name = "gucci" limit 10'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'show tables in db like abc'
        completed = util.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'show tables in db'
        completed = util.auto_complete_db(sql, db)
        assert completed == sql

        sql = 'show tables'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db'

        sql = 'show tables;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables like abc;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc;'

        sql = 'show tables like abc ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc ;'

        sql = 'show tables like \'abc\';'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'abc\';'

        sql = 'show tables like \'%ab_c\';'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'%ab_c\';'

        sql = 'desc my_table'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table'

        sql = 'desc my_table ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc my_table ;  '
        completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc db.my_table ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table ;'

        sql = 'desc db.my_table'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table'

        sql = 'show create table db.abc ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc ;'

        sql = 'show create table db.abc'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc'

        sql = 'show create table abc ;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc ;   '
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc;'
        completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc;'


    def test_is_exit_cmd(self):
        assert util.is_exit("exit")
        assert not util.is_exit("exit(")
        assert util.is_exit("exit()")
        assert util.is_exit("quit")
        assert util.is_exit("quit()")
        assert util.is_exit("QUIT()")


if __name__ == "__main__":
  unittest.main()
