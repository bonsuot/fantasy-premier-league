
import oracledb
from tqdm import tqdm
from collections import defaultdict

def upsert_insert_data(table_dict, cursor):
        
    """
    Update and Insert data into Oracle Database Tables.

    Args:
        table_dict (dict): Dictionary of table names and their corresponding DataFrames
        cursor: Oracle database cursor

    Returns:
        dict: Dictionary containing counts of updates and inserts for each table
    """
    
    # Dictionary to store counts for each table
    table_counts = defaultdict(lambda: {'updates': 0, 'inserts': 0, 'errors': 0})
    
    for table_name, df in table_dict.items():
        # Prepare the column names for the SQL statements
        columns = ', '.join(df.columns)
        primary_key = df.columns[0]
        update_columns = ', '.join([f"{col} = :{i+1}" for i, col in enumerate(df.columns) if col != primary_key])
        placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])

        print(f"\nProcessing table: {table_name}")
        
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
        print(f"\nResults for {table_name}:")
        print(f"  Updates: {table_counts[table_name]['updates']}")
        print(f"  Inserts: {table_counts[table_name]['inserts']}")
        print(f"  Errors:  {table_counts[table_name]['errors']}")
