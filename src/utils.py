import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import os

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

def get_aws_connection_str():



def get_psql_engine(connection_string):
	"""
	Connects to remote PSQL database with SQLAlchemy Engine.
	"""

	print('Connecting to remote PSQL database...')



	# Create engine
	engine = create_engine(connection_string)


	return engine


def push_psql(data, table_name, engine):
	"""
	Pushes data to remote PSQL database.

	:param data: (pd.DataFrame) -> the data
	:param table_name: (str) -> the table name
	:param engine: (sqlalchemy.engine) -> the SQLAlchemy engine
	"""

	print('Pushing data to remote PSQL database...')

	data.to_sql(table_name, engine, if_exists='replace', index=False)


def _table_exists(table_name, connection):

	check_query = f'''
		SELECT to_regclass('{table_name}');
	'''
	result = connection.execute(text(check_query))
	table_exists = result.scalar() is not None

	return table_exists

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
	connection = engine.connect()

	# Drop table if it already exists
	if _table_exists(new_table_name, connection):
		drop_query = f'''
			DROP TABLE {new_table_name};
		'''
		result = connection.execute(text(drop_query))


    # Create new table with specified fields
	quoted_fields = ', '.join([f'"{f}"' for f in fields])

	sql_query = f'''
	    CREATE TABLE {new_table_name} AS
	    SELECT {quoted_fields}
	    FROM {og_table_name};
	'''

	# Execute & commit changes
	connection.execute(text(sql_query))

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
	connection = engine.connect()

	# Ensure table exists
	if not _table_exists(table_name, connection):
		raise RuntimeError(f'{table_name} does not exist')

	for field, datatype in fields.items():
		sql = f"""
			ALTER TABLE {table_name}
			ADD "{field}" {datatype};
			"""
		try:
			connection.execute(text(sql))
			print(f"Column {field} added successfully.")
		except Exception as e:
			print(f"An error occurred: {e}")

	# Commit changes
	connection.commit()

	# Close connection
	connection.close()


def load_earthquake_data(earthquake_data_fpath):
	"""
	Loads data.

	:dearthquake_data_fpath: (str) -> the filepath
	"""

	print('Loading earthquake data...')
	
	data = pd.read_csv(earthquake_data_fpath)

	return data




