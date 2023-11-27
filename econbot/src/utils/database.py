import pandas as pd

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc as sqlalchemy_exc

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from prettytable import PrettyTable

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

"""
Display database
"""
def view_database(engine):
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
