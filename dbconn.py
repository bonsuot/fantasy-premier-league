import oracledb

cs = 'localhost/XEPDB1'

# connect to Oracle db
def connect_to_db():
        """
        create Oracle databse connection
        Returns:
            tuple: connection, cursor
        """
        try:

            try:
                oracledb.init_oracle_client()
            except Exception:
                    # If already initialized or thick mode not available, continue
                pass
            
            connection = oracledb.connect(user="your_username", password="password", dsn=cs)
            # Create a cursor
            cursor = connection.cursor()
            print("\n")
            print("=" * 50)
            print("Connection to Oracle database successful!")
            print("=" * 50)
            return connection, cursor

   
        except oracledb.Error as e:
            error_obj, = e.args
            print(f"Error Code: {error_obj.code}")
            print(f"Error Message: {error_obj.message}")
            return None, None

