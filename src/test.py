import unittest
import main

class Tester(unittest.TestCase):

    def test_auto_complete_db(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT * FROM apple.table where name = "gucci" limit 10'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'select * from .table where name = "gucci" limit 10'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'show tables in db like abc'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'show tables in db'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == sql 

        sql = 'show tables'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db'

        sql = 'show tables;'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db;'

        sql = 'show tables like abc;'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like abc;'

        sql = 'show tables like \'abc\';'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'abc\';'

        sql = 'show tables like \'%ab_c\';'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'show tables in my_db like \'%ab_c\';'

        sql = 'desc my_table'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table'

        sql = 'desc my_table ;'
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'

        sql = 'desc my_table ;  '
        _, completed = main.auto_complete_db(sql, db)
        assert completed == 'desc my_db.my_table ;'
      

    def test_is_exit_cmd(self):
        assert main.is_exit("exit")
        assert not main.is_exit("exit(")
        assert main.is_exit("exit()")
        assert main.is_exit("quit")
        assert main.is_exit("quit()")
        assert main.is_exit("QUIT()")


if __name__ == "__main__":
  unittest.main()
