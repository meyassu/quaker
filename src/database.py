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
import os

from exceptions import DatabaseConnectionError, AuthenticationTokenError, DataPushError, QueryExecutionError, TableExistenceError



# Load environment variables
load_dotenv()


"""
Establish database connection
"""
def get_engine_neon():
    """
    Connects to PSQL database on Neon with SQLAlchemy engine using credentials from .env file.
    (only for testing purposes)

    :return: (SQLAlchemy.engine) -> the engine
    """

    print('Connecting to Neon database...')

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

def get_engine_rds():
    """
    Connects to PSQL database on AWS RDS with SQLAlchemy Engine using credentials from .env file.

    :return: (SQLAlchemy.engine) -> the engine
    """

    print('Connecting to RDS instance...')

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
def init_database_neon(earthquake_data_fpath, big_table_name, small_table_name, small_fields, location_table_name, location_fields):
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

    print('Initializing Neon database...')

	# Load earthquake data
    earthquake_data = load_data_local(earthquake_data_fpath)


    # Get Neon engine
    neon_engine = get_engine_neon()
    write_table(data=earthquake_data, table_name=big_table_name, if_exists='replace', engine=neon_engine)

    # Filter original table
    filter_table(small_table_name=small_table_name, big_table_name=big_table_name, fields=small_fields, engine=neon_engine)

    # Create empty location table
    location_data = gpd.GeoDataFrame(columns=location_fields, geometry=location_fields[-1])
    write_table(data=location_data, table_name=location_table_name, if_exists='replace',  engine=neon_engine)


    return True


def init_database_rds(bucket_name, data_file_key, data_local_fpath, big_table_name, small_table_name, small_fields, location_table_name, location_fields):
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

    print('Initializing RDS instance database...')
    # Load earthquake data
    earthquake_data = load_data_s3(bucket_name, data_file_key, data_local_fpath)

    # Get RDS engine
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    username = os.getenv('PG_USERNAME')
    dbname = os.getenv('PG_DBNAME')
    region = os.getenv('REGION')

    rds_engine = get_engine_rds(host, port, username, dbname, region)
	

    # Create table in database
    write_table(data=earthquake_data, table_name=big_table_name, if_exists='replace', engine=rds_engine)


    # Filter original table
    filter_table(small_table_name=small_table_name, big_table_name=big_table_name, fields=small_fields, engine=rds_engine)

 
    # Create empty location table
    location_data = gpd.GeoDataFrame(columns=location_fields, geometry=location_fields[-1])
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

    print(f'Writing to {table_name}...')

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

    print(f'Filtering {fields} from {big_table_name} into {small_table_name}...')

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

    print(f'Adding {fields} to {table_name}...')

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
                print(f"Column {field} added successfully.")
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
    
    print(f'Dropping {fields} from {table_name}...')

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


def transfer_data(dest_table_name, src_table_name, fields, engine):
    """
    Transfers data between tables.

    :param dest_table_name: (str) -> the destination table name
    :param src_table_name: (str) -> the source table name
    :param fields: (list<str>) -> the fields to be transferred
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: (bool) -> indicates whether operation was sucessful
    """

    print(f'Tranferring {fields} from {src_table_name} to {dest_table_name}...')
    
    try:
        with engine.connect() as connection:
            quoted_fields = ', '.join([f'"{f}"' for f in fields])
            transfer_query = f'''
                    INSERT INTO {dest_table_name} ({quoted_fields})
                    SELECT {quoted_fields} FROM {src_table_name};
                    '''
            connection.execute(text(transfer_query))
            connection.commit()
            view_database(tables=['staging', 'earthquakes'], engine=engine)
            return True
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

    print(f'Clearing {table_name}...')

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

    print(f'Executing SELECT query: {query}...')

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

        for table_name in tables:
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
            with open(f'{table_name}.txt', 'w') as f:
                f.write(pretty_table.get_string())

            # Add a separator for better readability between tables
            print("\n" + "="*60 + "\n")
       
        # Close session
        session.close()


"""
Load source files for database
"""
def load_data_s3(bucket_name, file_key, local_fpath):
    """
    Loads data from S3 bucket.

    :param bucket_name: (str) -> the bucket name
    :param file_key: (str) -> the filename
    :param local_fpath: (str) -> the local filepath
    
    :return: (bool) -> indicates success of operation
    """

    print(f'Loading data from S3 @ {bucket_name}/{file_key}...')
 
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, file_key, local_fpath)
    except ClientError as e:
        raise ClientError(f'Error downloading file from S3: {e}')

    return True

def load_data_local(fpath):
    pass


