import cx_Oracle

class DbCon:
    def __init__(self, username, password, hostname, sid, port=1521):
        self.constr = f"{username}/{password}@{hostname}:{port}/{sid}"
        self.con = None
        
    def connect(self):
        self.con = cx_Oracle.connect(self.constr)
        
    def close(self):
        if self.con:
            self.con.close()
            self.con = None
            
    def sql_query(self, sql, params=""):
        with self.con.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return rows
        
    def call_plsql_function(self, function_name, return_data_type, parameters=None):
        """
        params:
            function_name - name of PL/SQL function to call
            return_data_type - the Python data type of the value returned by the function call.
                               Not a choice - needs to specify what it actually is.
                               This can be a cx_Oracle type e.g. cx_Oracle.DATETIME
            parameters - list containing parameters passed to the function
        Returns:
            whatever was received from the PL/SQL function call
        """
        with self.con.cursor() as cursor:
            value = cursor.callfunc(function_name, return_data_type, parameters)
        return value
        
    def call_plsql_procedure(self, procedure_name, input_params, ouput_param_types=None):
        # Tricky - possible to have output parameters but they need to be declared
        # and created using cursor.var() method
        # Not sure if below will work!
        if not output_param_types:
            output_param_types = []
        with self.con.cursor() as cursor:
            output_params = [cursor.var(t) for t in output_param_types]
            cursor.callproc(procedure_name, input_params+output_params)
        return output_params
