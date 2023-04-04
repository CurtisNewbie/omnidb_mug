import unittest
import main

class Tester(unittest.TestCase):
    def test_complete_database(self):
        db = 'my_db'
        sql = 'SELECT * FROM table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == 'SELECT * FROM my_db.table where name = "gucci" limit 10'

        sql = 'SELECT * FROM apple.table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == sql 

        sql = 'select * from .table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'show tables in db like abc'
        _, completed = main.complete_database(sql, db)
        assert completed == sql 

        sql = 'show tables in db'
        _, completed = main.complete_database(sql, db)
        assert completed == sql 

        sql = 'show tables'
        _, completed = main.complete_database(sql, db)
        assert completed == 'show tables in my_db'

if __name__ == "__main__":
  unittest.main()
