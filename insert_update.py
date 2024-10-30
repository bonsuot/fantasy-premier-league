
import oracledb
from tqdm import tqdm
from collections import defaultdict

def upsert_insert_data(table_name, df, cursor):
    '''insert or update table'''
    
    # Dictionary to store counts for each table
    table_counts = defaultdict(lambda: {'updates': 0, 'inserts': 0, 'errors': 0})
    
    # for table_name, df in table_dict.items():
    # Prepare the column names for the SQL statements
    columns = ', '.join(df.columns)
    primary_key = df.columns[0]
    update_columns = ', '.join([f"{col} = :{i+1}" for i, col in enumerate(df.columns) if col != primary_key])
    placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])

    # print(f"\nProcessing table: {table_name}")
    
    # MERGE statement for upsert
    merge_sql = f"""
    MERGE INTO {table_name} target
    USING (SELECT {', '.join([f':{i+1} AS {col}' for i, col in enumerate(df.columns)])} FROM dual) source
    ON (target.{primary_key} = source.{primary_key})
    WHEN MATCHED THEN
        UPDATE SET {update_columns}
    WHEN NOT MATCHED THEN
        INSERT ({columns})
        VALUES ({placeholders})
    """
        
    # Process each row with a progress bar
    for row in tqdm(df.itertuples(index=False, name=None), 
                    desc=f"Processing {table_name}", 
                    total=len(df)):
        try:
            # Execute the MERGE statement for each row
            cursor.execute(merge_sql, row)

            # Check the number of rows affected
            if cursor.rowcount == 1:
                # If rowcount is 1, either an update or an insert occurred
                if cursor.rowcount > 0:
                    # Increment insert or update count based on existence of the primary key in the target table
                    table_counts[table_name]['inserts'] += 1
                else:
                    # Otherwise, it's an update
                    table_counts[table_name]['updates'] += 1
            
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Error processing row in {table_name}: {error.message}")
            table_counts[table_name]['errors'] += 1

    # Print summary for this table
    # print(f"\nResults for {table_name}:")
    # print(f"  Updates: {table_counts[table_name]['updates']}")
    # print(f"  Inserts: {table_counts[table_name]['inserts']}")
    # print(f"  Errors:  {table_counts[table_name]['errors']}")

def insert_non_pk_data(table_name, df, cursor):

    table_counts = defaultdict(lambda: {'updates': 0, 'inserts': 0, 'errors': 0})

    # for table_name, df in (table_dict.items()):
        # Prepare the insert SQL statement with placeholders for each column
    columns = ', '.join(df.columns)
    placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"


        # Loop through DataFrame rows and insert into the table
    for row in tqdm(df.itertuples(index=False, name=None), 
                    desc=f"Processing {table_name}",
                     total=len(df)):
        try:
            cursor.execute(sql, row)
            if cursor.rowcount == 1:
                if cursor.rowcount > 0:
                    
                    table_counts[table_name]['inserts'] += 1
                else:
                
                    table_counts[table_name]['updates'] += 1
        except oracledb.DatabaseError as e:
            print(f"Error inserting data: {e}")

    # print(f"\nResults for {table_name}:")
    # print(f"  Updates: {table_counts[table_name]['updates']}")
    # print(f"  Inserts: {table_counts[table_name]['inserts']}")
    # print(f"  Errors:  {table_counts[table_name]['errors']}")