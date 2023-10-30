import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc as sqlalchemy_exc
from prettytable import PrettyTable
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os

from exceptions import DatabaseConnectionError, AuthenticationTokenError, DataPushError, QueryExecutionError, TableExistenceError


# Load environment variables
load_dotenv()

def get_neon_connection_str():
	"""
	Get Neon connection string.

	:return: None
	"""

	# Get credentials from .env file
	neon_user = os.getenv('NEON_PG_USER')
	neon_password = os.getenv('NEON_PG_PASSWORD')
	neon_host = os.getenv('NEON_PG_HOST')
	neon_port = os.getenv('NEON_PG_PORT')
	neon_database = os.getenv('NEON_PG_DATABASE')

	connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require'

	return connection_string



def get_psql_engine(host, port, username, dbname, region):
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

def push_psql(data, table_name, engine):
    """
    Pushes data to PSQL database on RDS instance.
`
    :param data: (pd.DataFrame) -> the data
    :param table_name: (str) -> the table name
    :param engine: (sqlalchemy.engine) -> the SQLAlchemy engine
    """

    print('Pushing data to PSQL database...')

    if not isinstance(data, pd.DataFrame):
        raise DataPushError('Provided data is not a pandas DataFrame')
    
    try:
        data.to_sql(table_name, engine, if_exists='replace', index=False)
    except sqlalchemy_exc.DBAPIError as e:      # Catch DB connection error
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: #
        raise DataPushError(f'Error pushing data to database: {e}')
    except Exception as e:                      # Catch-all
        raise DataPushError(f'Unexpected error: {e}')



def _table_exists(table_name, connection):

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
    except Exception as e:                    # Catch-all
        raise QueryExecutionError(f'Unexpected error: {e}')
    finally:
        connection.rollback()



def filter_table(new_table_name, og_table_name, fields, engine):
    """
    Creates subset table.

    :param new_table_name: (str) -> name of new table
    :param og_table_name: (str) -> name of original table
    :param fields: (list) -> fields to include in the new table
    :param engine: (SqlAlchemy.engine) -> object for remote database interaction

    :return: None
    """

    print('Filtering table...')

    # Open connection
    try:
        connection = engine.connect()
    except sqlalchemy_exc.DBAPIError as e: # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except Exception as e:
        raise DatabaseConnectionError(f'Unexpected error: {e}')
   finally:
        connection.close()

    # Drop table if it already exists
    if _table_exists(new_table_name, connection):
        drop_query = f'''
            DROP TABLE {new_table_name};
        '''
        try:
            connection.execute(text(drop_query))
        except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
            raise DatabaseConnectionError(f'Error connecting to database {e}')
        except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
            raise QueryExecutionError(f'Error executing query: {e}')
        except Exception as e:  
            raise QueryExecutionError(f'Unexpected error: {e}')
        finally:
            connection.rollback()

    # Create new table with specified fields
    quoted_fields = ', '.join([f'"{f}"' for f in fields])

    sql_query = f'''
        CREATE TABLE {new_table_name} AS
        SELECT {quoted_fields}
        FROM {og_table_name};
    '''

    # Execute & commit changes
    try:
        connection.execute(text(sql_query))
    except sqlalchemy_exc.DBAPIError as e:      # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database {e}')
    except sqlalchemy_exc.SQLAlchemyError as e: # Handle SQLAlchemy query execution error
        raise QueryExecutionError(f'Error executing query: {e}')
    except Exception as e:
        raise QueryExecutionError(f'Unexpected error: {e}')
    finally:
        connection.rollback()
        connection.close

    connection.commit()

    # Close connection
    connection.close()



def add_fields(table_name, fields, engine):
    """
    Adds fields to given table.

    :param table_name: (str) -> the name of the table
    :param fields: (list) -> the fields
    :param engine: (SQLAlchemy.engine) -> the engine

    :return: None
    """

    print('Adding fields...')


    # Open connection
    try:
        connection = engine.connect()
    except sqlalchemy_exc.DBAPIError as e: # Handle DB connection error
        raise DatabaseConnectionError(f'Error connecting to database: {e}')
    except Exception as e:
        raise DatabaseConnectionError(f'Unexpected error: {e}')
   finally:
        connection.close()

    # Ensure table exists
    if not _table_exists(table_name, connection):
        connection.close()
        raise TableExistenceError(f'{table_name} does not exist.')

    for field, datatype in fields.items():
        sql = f"""
            ALTER TABLE {table_name}
            ADD "{field}" {datatype};
            """
        try:
            connection.execute(text(sql))
            print(f"Column {field} added successfully.")
        except sqlalchemy_exc.DBAPIError as e:
            raise DatabaseConnectionError(f'Error connecting to database: {e}')
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise QueryExecutionError(f'Error executing query: {e}')
        except Exception as e:
            raise QueryExecutionError(f'Unexpected error: {e}')
        finally:
            connection.rollback()
            connection.close()

    # Commit changes
    connection.commit()

    # Close connection
    connection.close()


def view_database(engine):
    """
    Visualize all tables in the given database.

    :param engine: (sqlalchemy.engine) -> the SQLAlchemy engine maintaining a connection
    									  to the database

    :return: None							
    """
    
    # Estabish connection with database
    connection = engine.connect()

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

    # Close connection
    connection.close()



def load_earthquake_data_local(earthquake_data_fpath):
	"""
	Loads data.

	:dearthquake_data_fpath: (str) -> the filepath
	"""

	print('Loading earthquake data...')
	
	data = pd.read_csv(earthquake_data_fpath)

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

