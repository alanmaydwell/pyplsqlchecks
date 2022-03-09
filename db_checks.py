import unittest
import cx_Oracle # need to import this here to be able to specify cx_Oracle data types
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
        
        
    def test_concatenation(self):
        # Because first parameter is IN OUT, need to set type and value. 2nd is IN and can be simply set
        parameter_details = [{"type": cx_Oracle.STRING, "value": "start text"}, "extra text"]
        response = self.db_con.call_plsql_procedure("APPS.XXLSC_AM_VALIDATIONS_PKG.CONCAT_ERROR_MESSAGES", parameter_details)
        # Extract updated IN OUT value
        actual_result = response[0].getvalue()
        self.assertEqual(actual_result,  "start text~extra text", f"Unexpected concatenation result: {actual_result}")
        
        
    def tearDown(self):
        self.db_con.close()
        
        
if __name__ == '__main__':
    unittest.main()
