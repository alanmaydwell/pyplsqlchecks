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
        

    def call_plsql_procedure(self, procedure_name, param_details=None):
        """Call PL/SQL procuedure with optional parameters
        Note the way parameters are specified in param_details are different
        for IN, OUT and IN OUT parameters.
        
        IN - can just directly specify as a standard Python object
        OUT - need to be created as a cx_Oracle data type
        IN OUT - like OUT but also have initial value to be specified
        
        e.g. in the list below the 1st element is a simple IN parameter,
        the 2nd element is an OUT parameter with specified data type
        the 3rd element is an IN OUT parameter with both specified data type
        and value.
        
        ["one", {"type": cx_Oracle.STRING}, {"type": cx_Oracle.STRING, "value": "an IN OUT params"}]
              
        Parameters:
            procedure_name (str): name of procedure to call
            param_details (list/tuple): optional parameters to pass to the
            procedure note OUT and INOUT need to be specified in particular way.
            
        Returns:
            List containing the parameters supplied to the procedure. This
            includes any OUT values updated by the call.
        """
        with self.con.cursor() as cursor:
            # Sorting out params - need cursor for this to create cursor.var
            params = []
            if param_details is not None:
                params = self.make_plsql_parameters(param_details)
            cursor.callproc(procedure_name, params)
            return params
        
    def make_plsql_parameters(self, param_details):
        """Make parameters for PL/SQL procedure. 
        Helps with IN and IN OUT parameters which need to be cx_Oracle data types"""
        params = []
        with self.con.cursor() as cursor:
            for item in param_details:
                # Creating params that need specified data type (OUT or IN OUT)
                if isinstance(item, dict):
                    ptype = item.get("type", None)
                    if ptype:
                        param = cursor.var(ptype)
                        # Setting initial value for IN OUT params
                        value = item.get("value", None)
                        if value:
                            param.setvalue(0, value)
                        params.append(param)
                else:
                    # simple direct value (OK for IN parameters)
                    params.append(item)
        return params
            
               
    def call_concat_example(self):
        """
        Example of calling a PL/SQL procedure that has an IN parameter and an IN OUT parameter
        For simplicity just using hard coded values here.
        """
        with self.con.cursor() as cursor:
            # create IN OUT parameter with cx_Oracle data type - well also could be a OUT parameter at this point
            output_var = cursor.var(cx_Oracle.STRING)
            # Set its value. Initial 0 arg needed for this to work (also same for number)
            output_var.setvalue(0, "Hello")
            cursor.callproc("APPS.XXLSC_AM_VALIDATIONS_PKG.CONCAT_ERROR_MESSAGES", [output_var, "to you"])
        # See if the (IN)OUT value has changed.
        print(">>>", output_var.getvalue())

    
if __name__ == "__main__":
    # Trying things out
    
    # Connect to database
    cwa = DbCon("myusername", "mypassword", "cwa-db.aws.dev", "cwa", 1571)
    cwa.connect()
    
    # Run simple query
    result = cwa.sql_query("select sysdate from dual")
    print(result)
    
    # Call PL/SQL procedure with hard-coded parameters
    cwa.call_concat_example()
    
    # Call PL/SQL procedure with passed parameters, including INOUT, and receive the updated value
    parameter_details = [{"type": cx_Oracle.STRING, "value": "start-text"}, "extra-text"]
    resp = cwa.call_plsql_procedure("APPS.XXLSC_AM_VALIDATIONS_PKG.CONCAT_ERROR_MESSAGES", parameter_details)

    print(resp[0].getvalue())

    # End connection
    cwa.close()