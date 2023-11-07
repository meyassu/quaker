import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc as sqlalchemy_exc

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from prettytable import PrettyTable

from dotenv import load_dotenv
import os

from exceptions import DatabaseConnectionError, AuthenticationTokenError, DataPushError, QueryExecutionError, TableExistenceError



# Load environment variables
load_dotenv()


"""
Establish database connection
"""
def get_neon_engine():
    """
    Get Neon connection string.

    :return: None
    """

    # Get credentials from .env file
    user = os.getenv('NEON_PG_USER')
    password = os.getenv('NEON_PG_PASSWORD')
    host = os.getenv('NEON_PG_HOST')
    port = os.getenv('NEON_PG_PORT')
    database = os.getenv('NEON_PG_DATABASE')

    connection_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require'

    try:
        engine = create_engine(connection_url)
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection issues
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy error
        raise DatabaseConnectionError(f'Error creating SQLAlchemy engine: {e}')
    except Exception as e:                      # Catch-all
        raise DatabaseConnectionError(f'Unexpected error: {e}')

    return engine

def get_rds_engine(host, port, username, dbname, region):
    """
    Connects to PSQL database on AWS RDS with SQLAlchemy Engine.
    """

    print('Connecting to remote PSQL database...')

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
        query={'sslmode': 'verify-full', 'sslrootcert': '/home/ec2-user/rds-ca-2019-root.pem'})

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
def init_database_neon(earthquake_data_fpath, big_table_name, small_table_name, small_fields, extra_fields):
    """
    Create PostGreSQL database on Neon platform.

    :param earthquake_data_fpath: (str) -> the original earthquake data from Kaggle
    :param big_table_name: (str) -> name for table storing original earthquake data
    :param small_table_name: (str) -> name for smaller filtered table storing subset of fields
    :small_fields: (list<str>) -> subset of fields to be included in small table
    :param extra_fields: (dict <K: name, V: type>) extra fields to add to small table
                                                   e.g. {'Region': 'TEXT', 'Subregion': 'TEXT', 'Country': 'TEXT'}
    
    :return: (bool) -> indicates whether operation was successful
    """

	# Load earthquake data
    earthquake_data = load_earthquake_data_local(earthquake_data_fpath)


    # Get Neon engine
    neon_engine = get_neon_engine
    write_table(data=earthquake_data, table_name=big_table_name, engine=neon_engine)

    # Filter original table
    filter_table(small_table_name=small_table_name, big_table_name=big_table_name, fields=small_fields, engine=neon_engine)

    # Add geographic fields to filtered table
    add_fields(table_name="earthquakes", fields=extra_fields, engine=neon_engine)

    return True


def init_database_aws(bucket_name, data_file_key, data_local_fpath, big_table_name, small_table_name, small_fields, extra_fields):
    """
    Initializes PostGreSQL database on AWS RDS instance.

    :param bucket_name: (str) -> bucket name where earthquake data resides
    :param data_file_key: (str) -> data file key for earthquake data
    :param data_local_fpath: (str) -> local destination path for earthquake after being imported from S3
    :param big_table_name: (str) -> name for table storing original earthquake data
    :param small_table_name: (str) -> name for smaller filtered table storing subset of fields
    :small_fields: (list<str>) -> subset of fields to be included in small table
    :param extra_fields: (dict <K: name, V: type>) extra fields to add to small table
                                                   e.g. {'Region': 'TEXT', 'Subregion': 'TEXT', 'Country': 'TEXT'}
    
    :return: (bool) -> indicates whether operation was successful
    """

    print('Initializing database...')
    # Load earthquake data
    earthquake_data = load_earthquake_data_aws(bucket_name, data_file_key, data_local_fpath)

    # Get RDS engine
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    username = os.getenv('PG_USERNAME')
    dbname = os.getenv('PG_DBNAME')
    region = os.getenv('REGION')

    rds_engine = get_rds_engine(host, port, username, dbname, region)
	

    # Create table in database
    write_table(data=earthquake_data, table_name=big_table_name, engine=rds_engine)


    # Filter original table
    filter_table(small_table_name=small_table_name, big_table_name=big_table_name, fields=small_fields, engine=rds_engine)

    # Add geographic fields to filtered table
    add_fields(table_name=small_table_name, fields=extra_fields, engine=rds_engine)
    
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

    print('Creating table on PSQL database...')

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

    :return: None
    """

    print('Filtering table...')

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
            sql_query = f'''
                CREATE TABLE {small_table_name} AS
                SELECT {quoted_fields}
                FROM {big_table_name};
            '''
            connection.execute(text(sql_query))
            connection.commit()
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

    :return: None
    """

    print('Adding fields...')


    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
                raise TableExistenceError(f'{table_name} does not exist.')
            # Add fields
            for field, datatype in fields.items():
                query = f'''
                    ALTER TABLE {table_name}
                    ADD "{field}" {datatype};
                    '''
                connection.execute(text(query))
                print(f"Column {field} added successfully.")
            connection.commit()
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

    try:
        # Open connection
        with engine.connect() as connection:
            # Ensure table exists
            if not _table_exists(table_name, connection):
                raise TableExistenceError(f'{table_name} does not exist.')
            
            # Drop fields
            for field in fields:
                query = f'''
                    ALTER TABLE {table_name}
                    DROP "{field}";
                    '''
                connection.execute(text(query))
                print(f"Column {field} dropped successfully")
            
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
    finally:
        connection.rollback()


"""
Get data from database
"""
def get_data(query, engine):
    """
    Executes SELECT statement to get data from database.

    :param sql_query: (str) -> the SQL query
    :param engine: (SQLAlchemy.engine) -> the database engine
    """


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
def view_database(engine):
    """
    Visualize all tables in the given database.

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

        for table_name in metadata.tables.keys():
            print(f"\n=== Contents of table '{table_name}': ===\n")
        
            # Load table
            table = Table(table_name, metadata, autoload=True, autoload_with=engine)
            
            # Query table
            query = session.query(table)
            results = query.all()

            # Create PrettyTable instance with columns as field names
            pretty_table = PrettyTable([column.key for column in table.columns])

            # Add rows to PrettyTable instance
            for row in results:
                pretty_table.add_row(row)

            # Print table with data
            print(pretty_table)

            # Add a separator for better readability between tables
            print("\n" + "="*60 + "\n")
       
        # Close session
        session.close()


"""
Load source files for database
"""
def load_earthquake_data_local(earthquake_data_fpath):
    """
    Loads data.

    :earthquake_data_fpath: (str) -> the filepath
    """

    print('Loading earthquake data...')
    try:
        data = pd.read_csv(earthquake_data_fpath)
    except Exception as e:
        raise Exception(f'Error loading earthquake data: {e}')

    return data


def load_earthquake_data_aws(bucket_name, file_key, local_fpath):
    """
    Loads data from S3 bucket.

    :param earthquake_data_fpath: (str) -> the filepath
    """

    print('Loading earthquake data...')
 
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, file_key, local_fpath)
    except ClientError as e:
        raise ClientError(f'Error downloading file from S3: {e}')

    earthquake_data = pd.read_csv(local_fpath)

    return earthquake_data

# bucket_name = 'quakerbucket'
# data_file_key= 'earthquake_data.csv'
# data_local_fpath = '../data/earthquake_data.csv'






