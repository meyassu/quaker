import pandas as pd
from dotenv import load_dotenv
import sys
import os

from src.utils.database import get_db_engine, write_table
from src.utils.validate import validate_data

from src import INTERNAL_DATA_DIR
from src import LOGGER

if __name__ == '__main__':

    # Get .env fpath
    if len(sys.argv) < 2:
        print("Usage: python script.py <.env_filepath>")
        sys.exit(1)
    
    dotenv_fpath = sys.argv[1]
    load_dotenv(dotenv_fpath)

    # Get data
    rgdp_data = pd.read_csv(os.path.join(INTERNAL_DATA_DIR, 'rgdp.csv'))

    # Validate data
    is_valid, error_message = validate_data(rgdp_data)
    if is_valid == False:
        LOGGER.info(error_message)
        raise Exception(error_message)

    # Get database engine
    engine = get_db_engine()

    # Get desired table name
    table_name = os.getenv('ECONOMETRICS_TABLE_NAME')

    # Write data to table
    write_table(rgdp_data, table_name=table_name, if_exists='replace', engine=engine)




       





















