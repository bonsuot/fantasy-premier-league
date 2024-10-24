import oracledb
from tqdm import tqdm
import pandas as pd

"""
The create_table_query function dynamically creates the sql 
table statements query

The create_table function creates the tables in db 
"""

# SQL CREATE TABLE statement from DataFrame function
def create_table_query(df, table_name):

    """
    Function to create sql create table query

    Args:
        tabale name, dataframe
    """
    primary_key = df.columns[0] # set first column as the PK

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



def create_table(table_dict, cursor):
    """
    Function to create database tables

    Args:
        dictionary of tables and their data
        cursor
    """
    print("\nStarting tasks to create tables in Oracle db ...")
    print("=" * 50)
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
            except oracledb.DatabaseError as e:
                print(f"Table {table_name} already exists.")
                print(f"Error creating table: {e}")