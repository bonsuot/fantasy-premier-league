import cx_Oracle
from create_insert import *
# from dbconn import connect_to_db
from tqdm import tqdm
import pandas as pd

# connect to the database
# conn = connect_to_db()
# cursor = conn.cursor()

"""
The create_table_query function dynamically creates the sql 
table statements query

The create_table function creates the tables in db 
"""

# SQL CREATE TABLE statement from DataFrame function
def create_table_query(df, table_name):

    """
    Automatically choosing the first column of each 
    table as the primary key. It is important to keep
    the column you wish to use as the PK as first column
    in the operations.py script
    """
    primary_key = df.columns[0]

    sql = f"CREATE TABLE {table_name} ("
    
    # Loop over DataFrame columns and infer SQL data types
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas data types to Oracle SQL data types
        if pd.api.types.is_integer_dtype(dtype):
            sql += f"{col} NUMBER, "
        elif pd.api.types.is_float_dtype(dtype):
            sql += f"{col} FLOAT, "
        elif pd.api.types.is_bool_dtype(dtype):
            sql += f"{col} VARCHAR2(10), "
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql += f"{col} DATE, "
        else:
            sql += f"{col} VARCHAR2(255), "

    # add PK constraint  
    sql += f"PRIMARY KEY ({primary_key}), "

    # necessary to remove the last comma and space, and close the SQL statement
    sql = sql.rstrip(", ") + ")"
    
    return sql



def create_table_query_non_pk(df, table_name):

    """
    this is for tables that will violate PK constraint
    """
    # primary_key = df.columns[0]

    sql = f"CREATE TABLE {table_name} ("
    
    # Loop over DataFrame columns and infer SQL data types
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas data types to Oracle SQL data types
        if pd.api.types.is_integer_dtype(dtype):
            sql += f"{col} NUMBER, "
        elif pd.api.types.is_float_dtype(dtype):
            sql += f"{col} FLOAT, "
        elif pd.api.types.is_bool_dtype(dtype):
            sql += f"{col} VARCHAR2(10), "
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql += f"{col} DATE, "
        else:
            sql += f"{col} VARCHAR2(255), "

    # add PK constraint  
    # sql += f"PRIMARY KEY ({primary_key}), "

    # necessary to remove the last comma and space, and close the SQL statement
    sql = sql.rstrip(", ") + ")"
    
    return sql

"""
creating tables section
"""
def create_table(table_dict, cursor):
    for table_name, df in tqdm(table_dict.items(), desc="Creating tables"):
            
            cursor.execute(f"SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = UPPER(:1)", [table_name])
            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                print(f"Table {table_name} already exists. Skipping creation.")
                continue

            create_table_sql = create_table_query(df, table_name)

            # Execute the SQL statement to create the table
            try:
                cursor.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
            except cx_Oracle.DatabaseError as e:
                print(f"Table {table_name} already exists.")
                print(f"Error creating table: {e}")

# for non PK tables
def create_table_non_pk(table_dict, cursor):
    for table_name, df in tqdm(table_dict.items(), desc="Creating tables"):
            
            try:
                cursor.execute(f"DROP TABLE {table_name}")
                print(f"Table {table_name} dropped successfully.")
            except cx_Oracle.DatabaseError as e:
                print(f"Table {table_name} does not exist or could not be dropped: {e}")

            create_table_sql = create_table_query_non_pk(df, table_name)

            # Execute the SQL statement to create the table
            try:
                cursor.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
            except cx_Oracle.DatabaseError as e:
                print(f"Table {table_name} already exists.")
                print(f"Error creating table: {e}")

"""
Insert data into Oracle table row by row
"""
def insert_data(table_dict, cursor):
    for table_name, df in (table_dict.items()):
        # Prepare the insert SQL statement with placeholders for each column
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"


        # Loop through DataFrame rows and insert into the table
        for row in tqdm(df.itertuples(index=False, name=None), desc=f"Inserting data into {table_name}"):
            try:
                cursor.execute(sql, row)
            except cx_Oracle.DatabaseError as e:
                print(f"Error inserting data: {e}")