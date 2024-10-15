import cx_Oracle
from tqdm import tqdm
import pandas as pd
from dbconn import connect_to_db

conn = connect_to_db()
cursor = conn.cursor()

def execute_plsql_update(df_new, cursor, table_name):
    """
    Function to check for hash value changes and update rows in the specified table.
    Args:
        df_new (DataFrame): The new data DataFrame from API source
        cursor (cx_Oracle.Cursor): The cursor object for the database connection.
        table_name (str): The name of the table to update.
    """
    # again this only works by making the first column in
    # any table as the PK column
    primary_key_col = df_new.columns[0]
   
    # columns to iterate over excluding the PK/ROW_HASH column
    update_columns = [col for col in df_new.columns if col != primary_key_col and col != 'ROW_HASH']
    
    # Prepare the SET clause of the SQL statement
    # +3 for offset PK/ROW_HASH
    set_clause = ', '.join([f"{col} = :{i+3}" for i, col in enumerate(update_columns)])  

    
    for index, row in tqdm(df_new.iterrows(), total=len(df_new), desc=f"Processing updates for table {table_name}"):
        # PL/SQL block to check if the hash value is different and update the row
        plsql = f"""
        DECLARE
            v_pk {table_name}.{primary_key_col}%TYPE := :1;
            v_row_hash {table_name}.ROW_HASH%TYPE := :2;
            v_count NUMBER := 0;
        BEGIN
            -- Check if the hash value is different
            FOR rec IN (SELECT {primary_key_col}, ROW_HASH FROM {table_name} WHERE {primary_key_col} = v_pk) LOOP
                IF rec.ROW_HASH != v_row_hash THEN
                    -- If hash is different, update the row
                    UPDATE {table_name}
                    SET {set_clause},
                        ROW_HASH = v_row_hash
                    WHERE {primary_key_col} = rec.{primary_key_col};
                    
                    v_count := v_count + 1;
                END IF;
            END LOOP;
            
            :4 := v_count;
            COMMIT;
        END;
        """

        # Prepare the values for the update, including primary key and hash value
        bind_values = [row[primary_key_col], row['ROW_HASH']] + [row[col] for col in update_columns]
        
        v_count_out = cursor.var(int) # out parameter for the update count

        try:
            cursor.execute(plsql, bind_values + [v_count_out])
            
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(f"Error occurred on row {index}: {error.message}") 

    updates_count = v_count_out.getvalue()
    if updates_count >= 1:
            print(f"Updates were made on table {table_name}.")
            print(f"Total of {updates_count} row updates made on table {table_name}")
    else:
            print(f"No updates were made on table {table_name}.")
