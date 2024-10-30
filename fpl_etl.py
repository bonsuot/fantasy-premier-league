from prefect import task, flow, get_run_logger
from typing import Dict, List, Any
import logging
import sys 
from datetime import datetime
from contextlib import contextmanager
from insert_update import *
from create_database_table import *
from operations import *
from dbconn import connect_to_cloud_db

# Set up basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fpl_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Create logger
logger = logging.getLogger(__name__)

''' 
functions to check table existense 
'''
def table_exists(table_name: str, cursor: Any) -> bool:
    """Check if a table exists in the database"""
    logger = get_run_logger()
    
    try:
        cursor.execute(f"SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = UPPER(:1)", [table_name])
        
        exists = cursor.fetchone()[0]
        logger.info(f"Checked existence of table {table_name}: {'exists' if exists else 'does not exist'}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {str(e)}")
        raise

def check_tables_exist(tables_data: Dict[str, Any], cursor: Any) -> Dict[str, bool]:
    """Check existence of all required tables"""
    return {table_name: table_exists(table_name, cursor) 
             for table_name in tables_data.keys()}

'''******************************************************'''

@contextmanager
def get_db_connection():
    """Context manager for database connection with logging"""
    logger = get_run_logger()
    logger.info("Initiating database connection")
    try:
        conn, cursor = connect_to_cloud_db()
        logger.info("Database connection established successfully")
        yield conn, cursor
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise
    finally:
        logger.info("Closing database connection")
        try:
            cursor.close()
            conn.close()
            logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error while closing database connection: {str(e)}")

'''******************************************************'''

# first task for getting player ids
@task(retries=3, retry_delay_seconds=30)
def retrieve_player_id() -> List[int]:
    """Task to retrieve player IDs"""
    logger = get_run_logger()
    logger.info("Starting player ID retrieval")
    try:
        player_ids = get_player_ids()
        logger.info(f"Successfully retrieved {len(player_ids)} player IDs")
        return player_ids
    except Exception as e:
        logger.error(f"Failed to retrieve player IDs: {str(e)}")
        raise

# flow for data extraction for first task
@flow(name="extract_data")
def extract_flow(player_ids: List[int], mode: str = 'all') -> Dict[str, Any]:
    """Sub-flow handling all data extraction from API"""
    logger = get_run_logger()
    logger.info("Starting data extraction flow")
    
    data_points = {
        'gameweeks': get_gameweeks,
        'players': get_player_stat,
        'teams': get_team_stat,
        'positions': get_positions,
        'fixtures': lambda: get_fixtures(player_ids),
        'history': lambda: get_history(player_ids),
        'history_past': lambda: get_history_past(player_ids)
    }
    
    extracted_data = {}
    for name, getter in data_points.items():
        try:
            if mode == 'all': #or name in ['gameweeks', 'players', 'teams', 'positions']:
                logger.info(f"Extracting {name} data")
                extracted_data[name] = getter()
                logger.info(f"Successfully extracted {name} data")
            elif mode != 'all' and name in ['gameweeks', 'players', 'teams', 'positions']:
                logger.info(f"Extracting {name} data")
                extracted_data[name] = getter()
                logger.info(f"Successfully extracted {name} data")
            else:
                logger.info(f"Skipping {name} data extraction as mode is not 'all' ")
        except Exception as e:
            logger.error(f"Failed to extract {name} data: {str(e)}")
            raise

    logger.info("Completed data extraction flow")
    return extracted_data

# flow for data transform. Just passing the raw data for now
@flow(name="transform_data")
def transform_flow(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Sub-flow for any data transformations needed"""
    logger = get_run_logger()
    logger.info("Starting data transformation flow")
    try:
        # add transformation here later
        logger.info("Completed data transformation flow")
        return raw_data
    except Exception as e:
        logger.error(f"Error in transform flow: {str(e)}")
        raise

'''******************************************************'''

# task to create tables in the database
@task(retries=2)
def create_tables(tables_data: Dict[str, Any], 
                 cursor: Any, 
                 create_mode: str = 'auto') -> None:
    """
    Task to create database tables based on different modes
    
    Args:
        tables_data: Dictionary containing table data
        cursor: Database cursor
        mode: One of ['auto', 'force', 'skip', 'specific']
    """
    logger = get_run_logger()
    logger.info(f"Table creation started with mode: {create_mode}")
    
    pk_keys = {'gameweeks', 'players', 'teams', 'positions'}
    pk_data = {k: tables_data[k] for k in pk_keys if k in tables_data}

    non_pk_keys = {'fixtures', 'history', 'history_past'}
    non_pk_data = {k: tables_data[k] for k in non_pk_keys if k in tables_data}

    existing_pk_tables = check_tables_exist(pk_data, cursor)
    existing_non_pk_tables = check_tables_exist(non_pk_data, cursor)

    if create_mode == 'skip':
        logger.info("Skipping table creation as mode is 'skip'")
        return
    
    elif create_mode == 'force':
        logger.info("Force creating all tables")
        for table_name in pk_data.keys():
            if existing_pk_tables[table_name]:
                logger.info(f"Dropping existing table: {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")
                sql = create_table_query(tables_data[table_name], table_name)
                cursor.execute(sql)

        for table_name in non_pk_data.keys():
            if existing_non_pk_tables[table_name]:
                logger.info(f"Dropping existing table: {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")
                sql = create_non_pk_query(tables_data[table_name], table_name)
                cursor.execute(sql)

    elif create_mode == 'auto':
        logger.info("Auto-creating missing tables")
        for table_name, exists in existing_pk_tables.items():
            if not exists:
                logger.info(f"Creating missing table: {table_name}")
                sql = create_table_query(tables_data[table_name], table_name)
                cursor.execute(sql)
            else:
                logger.info(f"Skipping existing table: {table_name}")

        for table_name, exists in existing_non_pk_tables.items():
            if not exists:
                logger.info(f"Creating missing table: {table_name}")
                sql = create_non_pk_query(tables_data[table_name], table_name)
                cursor.execute(sql)
            else:
                logger.info(f"Skipping existing table: {table_name}")   
            
# flow for table creation
# @flow(name="create_table")
# def create_table_flow(transformed_data: Dict[str, Any], create_mode: str = 'auto') -> None:
#     """Sub-flow handling database operations"""
#     logger = get_run_logger()
#     logger.info("Starting table creation flow")
    
#     with get_db_connection() as (conn, cursor):
#         try:
#             logger.info("Beginning database transaction")
#             create_tables(transformed_data, cursor, create_mode)
#             # load_tables(transformed_data, cursor, load_mode)
            
#             conn.commit()
#             logger.info("Successfully committed database transaction")
            
#         except Exception as e:
#             logger.error(f"Error in create table flow, rolling back transaction: {str(e)}")
#             conn.rollback()
#             raise
#         finally:
#             logger.info("Completed table creation flow")

'''******************************************************'''

# task to load data into tables
@task(retries=2)
def load_tables(tables_data: Dict[str, Any], cursor: Any, mode: str = 'all') -> None:
    """Task to load data into tables"""
    logger = get_run_logger()

    pk_keys = {'gameweeks', 'players', 'teams', 'positions'}
    pk_data = {k: tables_data[k] for k in pk_keys if k in tables_data}

    non_pk_keys = {'fixtures', 'history', 'history_past'}
    non_pk_data = {k: tables_data[k] for k in non_pk_keys if k in tables_data}

    for table_name, data in pk_data.items():
        try:
            logger.info(f"Starting data load for table: {table_name}")
            rows_before = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            upsert_insert_data(table_name, data, cursor) # calling module for loading
            
            rows_after = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            rows_added = rows_after - rows_before
            
            logger.info(f"Loaded {rows_added} new rows into {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data for table {table_name}: {str(e)}")
            raise
    if mode == 'all':
        for table_name, data in non_pk_data.items():
            try:
                logger.info(f"Starting data load for table: {table_name}")
                rows_before = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                
                insert_non_pk_data(table_name, data, cursor) # calling module for loading
                
                rows_after = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                rows_added = rows_after - rows_before
                
                logger.info(f"Loaded {rows_added} new rows into {table_name}")
            except Exception as e:
                logger.error(f"Failed to load data for table {table_name}: {str(e)}")
                raise
    else:
        logger.info("Skipping non-primary key table data load as mode is not 'all'")

# flow for data loading
@flow(name="create_table_load_data")
def load_flow(transformed_data: Dict[str, Any],
              create_mode: str = 'auto', 
              load_mode: str = 'all') -> None:
    """Sub-flow handling database operations"""
    logger = get_run_logger()
    logger.info("Starting data load flow")
    
    with get_db_connection() as (conn, cursor):
        try:
            logger.info("Beginning database transaction")
            create_tables(transformed_data, cursor, create_mode)
            load_tables(transformed_data, cursor, load_mode)
            
            conn.commit()
            logger.info("Successfully committed database transaction")
            
        except Exception as e:
            logger.error(f"Error in load flow, rolling back transaction: {str(e)}")
            conn.rollback()
            raise
        finally:
            logger.info("Completed data load flow")

'''******************************************************'''

# how to handle failure task
@task
def handle_flow_failure(flow_name: str, error: str) -> None:
    """Task to handle flow failures"""
    logger = get_run_logger()
    logger.error(f"Flow {flow_name} failed with error: {error}")
    # will add some kind of notification here

'''******************************************************'''

"""
create_mode:
    auto: create all tables
    force: drop all tables and recreate
    skip: skip table creation
extract_mode:
    all: extract data for all tables
    if not all: extract data for (players, gameweeks, positions, teams)
load_mode:
    all: load data into all tables
    if not all: load data into only (players, gameweeks, positions, teams)

No paramater specification in main_flow equals default state (auto, all, all)
"""
# now the main flow to handle it all
@flow(name="fpl_etl_pipeline")
def main_flow(create_mode: str = 'auto', 
              extract_mode: str = 'all', 
              load_mode: str = 'all') -> None:
    """Main flow orchestrating the entire ETL pipeline"""
    logger = get_run_logger()
    start_time = datetime.now()
    logger.info(f"Starting FPL ETL pipeline at {start_time} with create_mode: {create_mode}, extract_mode: {extract_mode}, load_mode: {load_mode}")
    # logger.info(f"Starting FPL ETL pipeline at {start_time}")
    
    try:
        # Extract phase
        logger.info("Starting data extraction phase")
        player_ids = retrieve_player_id()
        raw_data = extract_flow(player_ids, extract_mode)
        logger.info("Completed data extraction phase")
        
        # Transform phase
        logger.info("Starting transformation phase")
        transformed_data = transform_flow(raw_data)
        logger.info("Completed transformation phase")

        # logger.info("Starting table creation phase")
        # create_table_flow(transformed_data, create_mode)
        # logger.info("Completed table creation phase")

        # Table Creation and Load phase
        logger.info("Starting table creation and loading phase")
        load_flow(transformed_data, create_mode, load_mode)
        logger.info("Completed load phase")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"FPL ETL pipeline completed successfully in {duration}")
        
    except Exception as e:
        logger.error("FPL ETL pipeline failed")
        handle_flow_failure("main_flow", str(e))
        raise
    finally:
        logger.info("ETL pipeline process ended")

'''******************************************************'''

@flow(name="retry_failed_flow")
def retry_specific_flow(flow_name: str, input_data: Any) -> Any:
    """Utility flow to retry specific failed sub-flows"""
    logger = get_run_logger()
    logger.info(f"Attempting to retry failed flow: {flow_name}")
    
    try:
        if flow_name == "extract_data":
            return extract_flow(input_data)
        elif flow_name == "transform_data":
            return transform_flow(input_data)
        elif flow_name == "load_data":
            return load_flow(input_data)
        else:
            raise ValueError(f"Unknown flow name: {flow_name}")
    except Exception as e:
        logger.error(f"Retry of {flow_name} failed: {str(e)}")
        handle_flow_failure(flow_name, str(e))
        raise

if __name__ == "__main__":
    main_flow()