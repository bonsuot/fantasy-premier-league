import oracledb
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access variables
username = os.getenv("ORACLE_USERNAME")
password = os.getenv("ORACLE_PASSWORD")
cs = os.getenv("ORACLE_DSN")
dir = os.getenv("lib_dir")


"""
Pointing databse connection to Oracle Cloud
For local connection, use commented code below this
"""
# connect to Oracle Cloud db
def connect_to_cloud_db():
        try:

            try:
                oracledb.init_oracle_client(lib_dir=dir)
            except Exception:
                    # If already initialized or thick mode not available, continue
                pass
            
            if not all([username, password]):
                    raise ValueError("Database credentials are not fully set in the environment variables.")    
            connection = oracledb.connect(user=username, password=password,
                                          dsn=cs)
                # Create a cursor
            cursor = connection.cursor()
            print(f"Successfully connected to Oracle Database at {datetime.now()}")
            return connection, cursor

        except oracledb.Error as e:
            error_obj, = e.args
            print(f"Error Code: {error_obj.code}")
            print(f"Error Message: {error_obj.message}")
            return None, None


# cs = 'localhost/XEPDB1' #connection string

# def connect_to_db():
#         """
#         create Oracle databse connection
#         Returns:
#             tuple: connection, cursor
#         """
#         try:

#             try:
#                 oracledb.init_oracle_client()
#             except Exception:
#                     # If already initialized or thick mode not available, continue
#                 pass
            
#             connection = oracledb.connect(user="your username", password="password", dsn=cs)
#             # Create a cursor
#             cursor = connection.cursor()
#             return connection, cursor
   
#         except oracledb.Error as e:
#             error_obj, = e.args
#             print(f"Error Code: {error_obj.code}")
#             print(f"Error Message: {error_obj.message}")
#             return None, None

