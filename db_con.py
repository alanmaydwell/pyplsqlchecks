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
        
    def call_plsql_procedure(self, procedure_name, in_params=None, out_param_types=None, inout_param_details=None):
        """

        Parameters
        ----------
        procedure_name : str
            name of PL/SQL procedure to call
        in_params : list/tuple, optional
            list containing input parameters
        out_param_types : list/tuple, optional
            list containing type of each output parameter
        inout_param_details : list of lists (or similar), optional
            list containing paired items - INOUT parameter data type and value
            e.g. [(cx_Oracle.STRING), "Initial words"), (cx_Oracle.NUMBER, 12)]

        Returns
        -------
        Output values from the called procedure, i.e.
        output_params, inout_params

        """
        # NEED TO UPDATE AS CAN'T COUNT ON PARAMS BEING IN ORDER: IN, OUT, INOUT
        
        # Safe defaulting params to empty lists
        if in_params is None:
            in_params = []
        if out_param_types is None:
            out_param_types = []
        if inout_param_details is None:
            inout_param_details = []
            
        with self.con.cursor() as cursor:
            # Make any OUT parameters from out_param_types
            out_params = [cursor.var(t) for t in out_param_types]
            # Make any INOUT parameters from inout_param_details
            inout_params = []
            for vartype, value in inout_param_details:
                # Need to create as cx_Oracle variable
                var = cursor.var(vartype)
                var.setvalue(0, value)
                inout_params.append(var)
            print(in_params)
            print(out_params)
            print(inout_params)
            print(in_params+out_params+inout_params)
            cursor.callproc(procedure_name, in_params+out_params+inout_params)
        print("}}}}", inout_params[0].getvalue())
        return out_params, inout_params


    def call_concat(self):
        """
        Example of calling a PL/SQL procedure that has an IN parameter and an IN OUT parameter
        For simplicity just using hard coded values here.
        """
        with self.con.cursor() as cursor:
            # create IN OUT parameter with cx_Oracle data type - well also could be a OUT parameter at this point
            output_var = cursor.var(cx_Oracle.STRING)
            # Set its value. Initial 0 arg needed for this to work (also same for number)
            output_var.setvalue(0, " Added Text")
            cursor.callproc("APPS.XXLSC_AM_VALIDATIONS_PKG.CONCAT_ERROR_MESSAGES", [output_var, "Initial text"])
        # See if the (IN)OUT value has changed.
        print(">>>", output_var.getvalue())
        return output_var.getvalue()

    
if __name__ == "__main__":
    cwa = DbCon("myusername", "mypassword", "cwa-db.aws", "cwa", 1521)
    cwa.connect()
    
    result = cwa.sql_query("select sysdate from dual")
    print(result)
    
    cwa.call_concat()
    #response = cwa.call_plsql_procedure("APPS.XXLSC_AM_VALIDATIONS_PKG.CONCAT_ERROR_MESSAGES", ["Start"], None, [(cx_Oracle.STRING, "End")])
    #print("R", response)
    cwa.close()
    

