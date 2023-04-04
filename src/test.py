import unittest
import main

class Tester(unittest.TestCase):
    def test_complete_database(self):
        db = 'my_db'
        sql = 'select * from apple.table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == sql 

        sql = 'select * from table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

        sql = 'select * from .table where name = "gucci" limit 10'
        _, completed = main.complete_database(sql, db)
        assert completed == 'select * from my_db.table where name = "gucci" limit 10'

if __name__ == "__main__":
  unittest.main()
