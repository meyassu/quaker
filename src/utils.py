import pandas as pd
import boto3
from botocore.exceptions import ClientError

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