import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc as sqlalchemy_exc

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from prettytable import PrettyTable

from dotenv import load_dotenv
import logging
import os

from exceptions import DatabaseConnectionError, AuthenticationTokenError, DataPushError, QueryExecutionError, TableExistenceError

"""
Constants
"""
DATA_DIR = '../../data/'

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), os.path.join(DATA_DIR, 'config/.env'))
load_dotenv(dotenv_path)


"""
Establish database connection
"""
def get_engine_rds():
    """
    Connects to PSQL database on AWS RDS with SQLAlchemy Engine using credentials from .env file.

    :return: (SQLAlchemy.engine) -> the engine
    """

    logging.log('Connecting to RDS instance...')

    region = os.getenv('REGION')
    host = os.getenv('RDS_PG_HOST')
    port = os.getenv('RDS_PG_PORT')
    username = os.getenv('RDS_PG_USER')
    dbname = os.getenv('RDS_PG_DATABASE')
    cert_fpath = os.getenv('RDS_PG_CERT_FPATH')

    try:
        # Generate an auth token
        rds_client = boto3.client('rds', region_name=region)
        auth_token = rds_client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=username,
            Region=region
        )
    except (NoCredentialsError, ClientError)  as e: # Handle AWS-side issues
        raise AuthenticationTokenError(f'Error generating authentication token: {e}')
    except Exception as e:                          # Catch-all
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

    try:
        engine = create_engine(connection_url)
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection issues
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy error
        raise DatabaseConnectionError(f'Error creating SQLAlchemy engine: {e}')
    except Exception as e:                      # Catch-all
        raise DatabaseConnectionError(f'Unexpected error: {e}')

    return engine


"""
Initialize database
"""
def init_database_rds(data_table_name, location_table_name):
    """
    Initializes database on AWS RDS instance by creating tables for the raw data
    containing the coordinates and the location table which will eventually store
    the results of the reverse geocoding algorithm.

    :param data_table_name: (str) -> the name of the data table
    :param location_table_name: (str) -> the name of the location table
    
    :return: (bool) -> indicates whether operation was successful
    """

    logging.log('Initializing RDS instance database...')
   
    # Load data
    data = pd.read_csv(os.path.join(DATA_DIR), 'data.csv')

    # Get RDS engine
    rds_engine = get_engine_rds()

    # Create table in database
    write_table(data=data, table_name=data_table_name, if_exists='replace', engine=rds_engine)

    # Create empty location table
    location_data = pd.DataFrame(columns=['Province', 'Country'])
    write_table(data=location_data, table_name=location_table_name, if_exists='replace', engine=rds_engine)

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

    logging.log(f'Writing to {table_name}...')

    if not isinstance(data, pd.DataFrame):
        raise DataPushError('Provided data is not a pandas DataFrame')
    
    try:
        data.to_sql(table_name, engine, if_exists=if_exists, index=False)
    except sqlalchemy_exc.DBAPIError as e:      # Catch DB connection error
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: #
        raise DataPushError(f'Error pushing data to database: {e}')
    except Exception as e:                      # Catch-all
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

    logging.log(f'Filtering {fields} from {big_table_name} into {small_table_name}...')

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
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle query execution error
        raise QueryExecutionError(f'Error execuring query: {e}') 
    except Exception as e:
        raise Exception(f'Unexpected error: {e}')

def add_fields(table_name, fields, engine):
    """
    Adds fields to given table.

    :param table_name: (str) -> the name of the table
    :param fields: (dict <K: field_name, V: datatype>) -> the fields
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: (bool) -> indicates whether operation was sucessful
    """

    logging.log(f'Adding {fields} to {table_name}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
                raise TableExistenceError(f'{table_name} does not exist.')
            # Add fields
            for field, datatype in fields.items():
                add_query = f'''
                    ALTER TABLE {table_name}
                    ADD "{field}" {datatype};
                    '''
                connection.execute(text(add_query))
                logging.log(f"Column {field} added successfully.")
            connection.commit()
            return True
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e:  # Handle query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:
        raise Exception(f'Unexpected error: {e}')

def drop_fields(table_name, fields, engine):
    """
    Drop fields from database.

    :param table_name: (str) -> the name of the table
    :param fields: (list<st>) -> the fields that need to be dropped
    :param engine: (SQLAlchemy.engine) -> the database engine 
    """
    
    logging.log(f'Dropping {fields} from {table_name}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
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
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e:  # Handle query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:
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
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        raise QueryExecutionError(f'Unexpected error: {e}')

def clear_table(table_name, engine):
    """
    Clears table.

    :param table_name: (str) -> the target table
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: (bool) -> indicates whether operation was sucessful
    """

    logging.log(f'Clearing {table_name}...')

    try:
        with engine.connect() as connection:
            clear_query = text(f"DELETE FROM {table_name};")
            connection.execute(clear_query)
            connection.commit()
            return True
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        raise QueryExecutionError(f'Unexpected error: {e}')
    

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

    logging.log(f'Executing SELECT query: {query}...')

    try:
        # Open connection
        with engine.connect() as connection:
            # Execute the SQL query and fetch the results into a Pandas DataFrame
            result = connection.execute(text(query))
            data = pd.DataFrame(result.fetchall(), columns=result.keys())
            return data
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:                      # Catch-all
        raise QueryExecutionError(f'Unexpected error: {e}')


"""
Display database
"""
def view_database(tables, engine):
    """
    View all tables in the given database.

    :param engine: (sqlalchemy.engine) -> the SQLAlchemy engine maintaining a connection
    									  to the database

    :return: None							
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
            print(f"\n=== Contents of table '{table_name}': ===\n")
        
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
            print(pretty_table)
            with open(f'{table_name}.txt', 'w') as f:
                f.write(pretty_table.get_string())

            # Add a separator for better readability between tables
            print("\n" + "="*60 + "\n")
       
        # Close session
        session.close()