import pandas as pd
from dotenv import load_dotenv
import os

from ..utils.database import get_engine_rds, write_table

"""
Constants
"""
DATA_INTERNAL_DIR = '../../data/internal'
DATA_INPUT_DIR = '../../data/user/input'
DATA_CONFIG_DIR = '../../data/user/config'

ECONOMETRICS_TABLE_NAME = os.get_env('ECONOMETRICS_TABLE_NAME')

dotenv_path = os.path.join(os.path.dirname(__file__), os.path.join(DATA_CONFIG_DIR, '.env'))
load_dotenv(dotenv_path)

if __name__ == '__main__':
    
    # Get data
    rgdp_data = pd.read_csv(os.path.join(DATA_INTERNAL_DIR), 'rgdp.csv')

    # Get RDS engine
    rds_engine = get_engine_rds()

    # Write data to table
    write_table(rgdp_data, table_name=ECONOMETRICS_TABLE_NAME, if_exists='replace', engine=rds_engine)
       





















