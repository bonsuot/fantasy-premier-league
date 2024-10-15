import cx_Oracle

# connect to Oracle db
def connect_to_db():
    try:
        dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='XEPDB1') 
        connection = cx_Oracle.connect(user='your_username', password='your_password', dsn=dsn_tns)
        print("Connected to Database successfully!")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Database: {e}")
        return None

# if __name__ == "__main__":
#     connect_to_db()

