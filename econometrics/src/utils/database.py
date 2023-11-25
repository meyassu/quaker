import pandas as pd
import geopandas as gpd
import numpy as np

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc as sqlalchemy_exc

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from prettytable import PrettyTable

from dotenv import load_dotenv
import os

from src.utils.exceptions import DatabaseConnectionError, AuthenticationTokenError, DataPushError, QueryExecutionError, TableExistenceError
from src import LOGGER

"""
Establish database connection
"""
def get_db_engine():
    """
    Connects to PostGreSQL database with SQLAlchemy Engine using credentials from .env file.

    :return: (SQLAlchemy.engine) -> the engine
    """

    LOGGER.info('Connecting to database...')
    print('Connecting to database...', flush=True) 
    
    # Get basic database information
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    username = os.getenv('DB_USER')
    dbname = os.getenv('DB_NAME')

    connection_url = None

    is_rds = os.getenv('RDS') == 'TRUE'
    
    # Build RDS connection URL if  database is on RDS instance
    cert_path = None
    region = None
    if is_rds:
        region = os.getenv('REGION')
        cert_fpath = os.getenv('DB_CERT_FPATH')
    
        try:
            # Generate an auth token to use as password
            rds_client = boto3.client('rds', region_name=region)
            auth_token = rds_client.generate_db_auth_token(
                DBHostname=host,
                Port=port,
                DBUsername=username,
                Region=region
        )
        except (NoCredentialsError, ClientError)  as e: # Handle AWS-side issues
            LOGGER.error(f'Error generating authentication token: {e}')
            raise AuthenticationTokenError(f'Error generating authentication token: {e}')
        except Exception as e:                          # Catch-all
            LOGGER.error(f'Unexpected error: {e}')
            raise AuthenticationTokenError(f'Unexpected error: {e}')
    
    
        # Construct connection string
        connection_url = URL (
            drivername='postgresql+psycopg2',
            username=username,
            password=auth_token,
            host=host,
            port=port,
            database=dbname,
            query={'sslmode': 'verify-full', 'sslrootcert': cert_fpath})

    # Build generic database connection URL otherwise
    else:
        password = os.getenv('DB_PASSWORD')
        connection_url = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}?sslmode=require'

    try:
        engine = create_engine(connection_url)
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection issues
        LOGGER.error(f'Error connecting to database: {e}')
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy error
        LOGGER.error(f'Error creating SQLAlchemy engine: {e}')
        raise DatabaseConnectionError(f'Error creating SQLAlchemy engine: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise DatabaseConnectionError(f'Unexpected error: {e}')

    return engine


"""
Initialize database
"""
def init_database(data, data_table_name, location_table_name, engine):
    """
    Initializes database on AWS RDS instance by creating tables for the raw data
    containing the coordinates and the location table which will eventually store
    the results of the reverse geocoding algorithm.

    :param data_table_name: (str) -> the name of the data table
    :param location_table_name: (str) -> the name of the location table
    :param engine: (SQLAlchemy.engine) -> the database engine
    
    :return: (bool) -> indicates whether operation was successful
    """

    LOGGER.info('Initializing database...')
    print('Initializing database...', flush=True)
    
    # Create table in database (after lower-casing all field names for simplicity and adding province/country columns)
    data.columns = [col.lower() for col in data.columns]
    write_table(data=data, table_name=data_table_name, if_exists='replace', engine=engine)
    add_fields(table_name=data_table_name, fields={'province': 'TEXT', 'country':'TEXT'}, engine=engine) 


    # Create empty location table
    location_data = pd.DataFrame(columns=['province', 'country'])
    write_table(data=location_data, table_name=location_table_name, if_exists='replace', engine=engine)

    return True


"""""
Modify database
"""
def write_table(data, table_name, if_exists, engine):
    """
    Pushes data to PSQL database on RDS instance.

    :param data: (pd.DataFrame) -> the data
    :param table_name: (str) -> the table name
    :param engine: (sqlalchemy.engine) -> the SQLAlchemy engine
    """

    print(f'Writing to {table_name}...')
    LOGGER.debug(f'Writing to {table_name}...')

    if not isinstance(data, pd.DataFrame):
        LOGGER.error('Provided data is not a pandas DataFrame')
        raise DataPushError('Provided data is not a pandas DataFrame')
    
    try:
        data.to_sql(table_name, engine, if_exists=if_exists, index=False)
    except sqlalchemy_exc.DBAPIError as e:      # Catch DB connection error
        LOGGER.error(f'Error connecting to database: {e}')
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Catch DB errors
        LOGGER.error(f'Error pushing data to database: {e}')
        raise DataPushError(f'Error pushing data to database: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise DataPushError(f'Unexpected error: {e}')

def filter_table(small_table_name, big_table_name, fields, engine):
    """
    Creates subset table.

    :param new_table_name: (str) -> name of new table
    :param og_table_name: (str) -> name of original table
    :param fields: (list) -> fields to include in the new table
    :param engine: (SqlAlchemy.engine) -> object for remote database interaction

    :return: (bool) -> indicates whether operation was sucessful
    """

    LOGGER.debug(f'Filtering {fields} from {big_table_name} into {small_table_name}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Drop table if it already exists
            if _table_exists(small_table_name, connection):
                drop_query = f'''
                    DROP TABLE {small_table_name};
                '''
                connection.execute(text(drop_query))
            
            # Create new table with specified fields
            quoted_fields = ', '.join([f'"{f}"' for f in fields])
            create_query = f'''
                CREATE TABLE {small_table_name} AS
                SELECT {quoted_fields}
                FROM {big_table_name};
            '''
            connection.execute(text(create_query))
            connection.commit()
            return True
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database: {e}')
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle query execution error
        LOGGER.error(f'Error execuring query: {e}')
        raise QueryExecutionError(f'Error execuring query: {e}') 
    except Exception as e:
        LOGGER.error(f'Unexpected error: {e}')
        raise Exception(f'Unexpected error: {e}')

def add_fields(table_name, fields, engine):
    """
    Adds fields to given table.

    :param table_name: (str) -> the name of the table
    :param fields: (dict <K: field_name, V: datatype>) -> the fields
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: (bool) -> indicates whether operation was sucessful
    """

    LOGGER.debug(f'Adding {fields} to {table_name}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
                LOGGER.error(f'{table_name} does not exist.')
                raise TableExistenceError(f'{table_name} does not exist.')
            # Add fields
            for field, datatype in fields.items():
                add_query = f'''
                    ALTER TABLE {table_name}
                    ADD "{field}" {datatype};
                    '''
                connection.execute(text(add_query))
                LOGGER.debug(f"Column {field} added successfully.")
            connection.commit()
            return True
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database: {e}')
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e:  # Handle query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:
        LOGGER.error(f'Unexpected error: {e}')
        raise Exception(f'Unexpected error: {e}')

def drop_fields(table_name, fields, engine):
    """
    Drop fields from database.

    :param table_name: (str) -> the name of the table
    :param fields: (list<st>) -> the fields that need to be dropped
    :param engine: (SQLAlchemy.engine) -> the database engine 
    """
    
    LOGGER.debug(f'Dropping {fields} from {table_name}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
                LOGGER.error(f'{table_name} does not exist.')
                raise TableExistenceError(f'{table_name} does not exist.')
            
            # Drop fields
            for field in fields:
                drop_query = f'''
                    ALTER TABLE {table_name}
                    DROP "{field}";
                    '''
                connection.execute(text(drop_query))
            connection.commit()
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database: {e}')
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e:  # Handle query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:
        LOGGER.error(f'Unexpected error: {e}')
        raise Exception(f'Unexpected error: {e}')


def _table_exists(table_name, connection):
    """
    Checks if passed table exists.

    :param table_name: (str) -> the table name
    :param connection: (SQLAlchemy.connection) -> the database connection

    :return: (bool) -> indicates table existence
    """

    check_query = f'''
        SELECT to_regclass('{table_name}');
    '''
    try:
        result = connection.execute(text(check_query))
        table_exists = result.scalar() is not None
        return table_exists
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database {e}')
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise QueryExecutionError(f'Unexpected error: {e}')

def clear_table(table_name, engine):
    """
    Clears table.

    :param table_name: (str) -> the target table
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: (bool) -> indicates whether operation was sucessful
    """

    LOGGER.debug(f'Clearing {table_name}...')

    try:
        with engine.connect() as connection:
            clear_query = f'DELETE FROM {table_name};'
            connection.execute(text(clear_query))
            connection.commit()
            return True
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database {e}')
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise QueryExecutionError(f'Unexpected error: {e}')
    

def merge_tables(static_table_name, merging_table_name, fields, engine):
    """"
    Merges columns from merging table into static table.
    (Presently limited to merging only two fields into static table)

    :param static_table_name: (str) -> the name of the static table
    :param merging_table_name: (str) -> the name of the merging table
    :param fields: (list) -> the fields to be merged into the static table
    :param engine: (SQLAlchemy.engine) -> the database engine

    :return: (bool) -> indicates the success of the operation
    """

    LOGGER.debug(f'Merging {fields} from {merging_table_name} into {static_table_name}')
    print(f'Merging {fields} from {merging_table_name} into {static_table_name}', flush=True)

    # Query to set up temporary keys in preparation for merging process
    static_key_query = f'''
                        ALTER TABLE {static_table_name} ADD COLUMN temp_id SERIAL;
                        '''
    
    merging_key_query = f'''
                        ALTER TABLE {merging_table_name} ADD COLUMN temp_id SERIAL;
                        '''
    
    # Query to perform the merger
    merging_query = f'''
                    UPDATE {static_table_name}
                    SET {fields[0]} = {merging_table_name}.{fields[0]},
                        {fields[1]} = {merging_table_name}.{fields[1]}
                    FROM {merging_table_name}
                    WHERE {static_table_name}.temp_id = {merging_table_name}.temp_id;
                    '''

    # Query to remove the temporary keys
    static_remkey_query = f'''
                           ALTER TABLE {static_table_name} DROP COLUMN temp_id;
                           '''
    merging_remkey_query = f'''
                           ALTER TABLE {merging_table_name} DROP COLUMN temp_id;
                           '''
    

    # Execute all the queries in sequence
    try:
        with engine.connect() as connection:
            # Create temporary keys in each table in preparation for merging process
            connection.execute(text(static_key_query))
            connection.execute(text(merging_key_query))

            # Perform the merger
            connection.execute(text(merging_query))

            # Remove the temporary keys
            connection.execute(text(static_remkey_query))
            connection.execute(text(merging_remkey_query))

            # Commit transation
            connection.commit()
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database {e}')
        LOGGER.error(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise QueryExecutionError(f'Unexpected error: {e}')

    return True


"""
Get data from database
"""
def get_data(query, engine):
    """
    Executes SELECT statement to get data from database.

    :param sql_query: (str) -> the SQL query
    :param engine: (SQLAlchemy.engine) -> the database engine
    
    :return: (pd.DataFrame) -> the data
    """

    LOGGER.debug(f'Executing SELECT query: {query}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Execute the SQL query and fetch the results into a Pandas DataFrame
            result = connection.execute(text(query))
            data = pd.DataFrame(result.fetchall(), columns=result.keys())
            return data
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        LOGGER.error(f'Error connecting to database {e}')
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        LOGGER.error(f'Error executing query: {e}')
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        LOGGER.error(f'Unexpected error: {e}')
        raise QueryExecutionError(f'Unexpected error: {e}')


"""
Display database
"""
def view_database(tables, engine):
    """
    View all tables in the given database.

    :param engine: (sqlalchemy.engine) -> the SQLAlchemy engine maintaining a connection
    									  to the database

    :return: (bool) -> indicates success of operation						
    """
    
    # Open connection
    with engine.connect() as connection:
        # Retrieve metadata about database
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Create a new session
        Session = sessionmaker(bind=engine)
        session = Session()

        for table_name in metadata.tables:
            print(f"\n=== Contents of table '{table_name}': ===\n", flush=True)
        
            # Load table
            table = metadata.tables[table_name]
            
            # Query table
            query = session.query(table)
            results = query.all()

            # Create PrettyTable instance with columns as field names
            pretty_table = PrettyTable([column.key for column in table.columns])

            # Add rows to PrettyTable instance
            for row in results:
                pretty_table.add_row(row)

            # print table with data
            print(pretty_table, flush=True)
            with open(f'{table_name}.txt', 'w') as f:
                f.write(pretty_table.get_string())

            # Add a separator for better readability between tables
            print("\n" + "="*60 + "\n", flush=True)
       
        # Close session
        session.close()

    
    return True
