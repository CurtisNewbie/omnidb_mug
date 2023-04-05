import unittest
import util

class Tester(unittest.TestCase):

    def test_auto_complete_db(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        _, completed = util.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

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
