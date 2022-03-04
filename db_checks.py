import unittest
from db_con import DbCon
from con_data import username, password, sid, hostname, port 

class TestCWA(unittest.TestCase):
    def setUp(self):
        self.db_con = DbCon(username, password, hostname, sid, port)
        self.db_con.connect()
        
    def test_check_database_version(self):
        # Note end is "64bi" without final "t" 
        expected_version = "Oracle Database 10g Enterprise Edition Release 10.2.0.4.0 - 64bi"
        result = self.db_con.sql_query("select * from v$version")
        reported_version = result[0][0]
        self.assertEqual(reported_version, expected_version, f"Unexpected database version: {reported_version}")
        
    def tearDown(self):
        self.db_con.close()
        
        
if __name__ == '__main__':
    unittest.main()
