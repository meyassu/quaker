import pandas as pd
from utils import load_earthquake_data

def _format_date(date):
	"""
	Helper function to func:format_dates.
	Formats given date to be MM/DD/YYYY.

	:param date: (str) -> the date

	:return: (str) -> the reformatted date
	"""

    
	parts = date.split('/')
	if len(parts) == 3:
		month, day, year = parts

		# Ensure month and day are two digits
		parts[0] = month.zfill(2)
		parts[1] = day.zfill(2)

		print(month)

		year = int(year)

		# Handle 20th/21st century years separately
		if 0 <= year <= 99:
			if 65 <= year <= 99:
				parts[2] = '19' + parts[2]
			else:
				parts[2] = '20' + parts[2].zfill(2)  # Add leading zeros if needed
		else:
			return date
	else:
		return date

	# Reconstruct date string and return
	return '/'.join(parts)


def format_dates(earthquake_df, date_column='Date'):
    """
    Formats dates to be MM/DD/YYYY.

    :param earthquake_df: (pd.DataFrame) The DataFrame containing your data
    :param date_column: (str) The name of the column containing the dates

    :return: (pd.Dataframe) -> the reformatted dataframe
    """

    print("Formatting dates...")

    # Apply the conversion function to the date column
    earthquake_df[date_column] = earthquake_df[date_column].apply(_format_date)

    return earthquake_df



# Load earthquake data
earthquake_data_fpath = '../data/earthquake_data.csv'
earthquake_data = load_earthquake_data(earthquake_data_fpath)

format_dates(earthquake_df).to_csv('earthquake_data_date_formatted.csv', index=False)

