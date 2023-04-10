import unittest
import util

class Tester(unittest.TestCase):

    def test_escape_sql(self):
        e = util.escape('select * from table where f = "12345";')
        assert e == 'select * from table where f = \\"12345\\";'

        e = util.escape('select * from table where f = \'12345\';')
        assert e == 'select * from table where f = \'12345\';'


    def test_auto_complete_db(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT * FROM table where name in ("gucci", "juice") limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name in ("gucci", "juice") limit 10'

        sql = 'SELECT * FROM table where f_name in ("gucci", "juice") limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where f_name in ("gucci", "juice") limit 10'

        sql = 'SELECT count(*) FROM table where name = "gucci" limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT count(*) FROM table;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table;'

        sql = 'SELECT name, desc FROM table;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT name, desc FROM my_db.table;'

        sql = 'SELECT name  FROM table;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT name  FROM my_db.table;'

        sql = 'SELECT count(*) FROM table ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT count(*) FROM my_db.table ;'

        sql = 'SELECT * FROM apple.table where name = "gucci" limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'select * from .table where name = "gucci" limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'show tables in db like abc'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'show tables in db'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'show tables'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db'

        sql = 'show tables;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables like abc;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc;'

        sql = 'show tables like abc ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc ;'

        sql = 'show tables like \'abc\';'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'abc\';'

        sql = 'show tables like \'%ab_c\';'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'%ab_c\';'

        sql = 'desc my_table'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table'

        sql = 'desc my_table ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc my_table ;  '
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc db.my_table ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table ;'

        sql = 'desc db.my_table'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'desc db.my_table'

        sql = 'show create table db.abc ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc ;'

        sql = 'show create table db.abc'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table db.abc'

        sql = 'show create table abc ;'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc ;   '
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'show create table my_db.abc ;'

        sql = 'show create table abc;'
        _, completed = util.auto_complete_db(sql, db)
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
