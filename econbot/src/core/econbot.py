import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from collections import defaultdict

import logging
import time
import os



"""
Constants
"""
BASE_URL_RGDP = 'https://fred.stlouisfed.org/searchresults/?st=gdp&t={}&ob=sr&od=desc'
DATA_INTERNAL_DIR = '../../data/internal'
DATA_INPUT_DIR = '../../data/user/input'
DATA_OUTPUT_DIR = '../../data/user/output'
DATA_CONFIG_DIR = '../../data/user/config'


def get_driver():
    """
    Set up Chrome driver.

    :param download_dir: (str) -> the destination directory for downloads

    :return: (selenium.webdriver) -> the web driver
    """
   
    logging.log('Setting up Chrome driver...')

    # Set preferences for chrome driver
    chrome_options = Options()

    prefs = {
        "download.default_directory": DATA_INTERNAL_DIR,
        "download.prompt_for_download": False, # Enable auto-download
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    try:
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        logging.error('Error instantiating Chrome driver: {e}')
        raise
    
    return driver


def get_rgdp_data(country_codes, driver):
    """
    Collect country-level rGDP data. 

    :param country_codes: (list<str>) -> the codes for the target countries
    :param driver: (selenium.webdriver) -> the Chrome driver

    :return: (bool) -> a boolean indicating the success of the operation
    """

    logging.log(f'Getting rGDP data for {len(country_codes)} countries...')

    keywords_hierarchy = ['Real GDP at Constant National Prices', 'Real Gross Domestic Product for',' Gross Domestic Product for', 'GDP']

    try:
        for i, c in enumerate(country_codes):
            logging.log(f'Downloading file for {c}... ({i+1} / {len(country_codes)})')
            
            try:
                # Go to specific country data page
                url = BASE_URL_RGDP.format(c)
                driver.get(url)
                time.sleep(5)

                # Rank search results according to data quality
                results_ranking = defaultdict(int)
                best_result_link = None
                best_result_title = None
                search_results = driver.find_elements(By.CLASS_NAME, 'search-series-title-gtm')
                for item in search_results:
                    item_title = item.get_attribute('aria-label')
                    item_link = item.get_attribute('href')
                    for i, k in enumerate(keywords_hierarchy):
                        if k in item_title:
                            results_ranking[(item_title, item_link)] = i
                            break
                
                # Identify highest quality data link
                sorted_results_ranking = sorted(results_ranking.items(), key=lambda x: x[1])
                if sorted_results_ranking:
                    best_result =  sorted_results_ranking[0][0]
                    best_result_title, best_result_link = best_result
                
                # Go to highest quality data link
                driver.get(best_result_link)
                time.sleep(5)

                # Click download button
                download_button = driver.find_element(By.ID, 'download-button')
                download_button.click()
                time.sleep(2)

                # Download file
                csv_download_link = driver.find_element(By.ID, 'download-data-csv')
                driver.get(csv_download_link.get_attribute('href'))
                time.sleep(7)
                
                # Determine filename
                downloaded_fname = f'{c.replace("%20", " ").title()}_{best_result_title}_DOWNLOADED.csv'
                downloaded_fpath = os.path.join(DATA_INTERNAL_DIR, downloaded_fname)

                # Rename file by waiting for it to appear in filesystem, then calling os.rename
                temp_fpath = None 
                while not temp_fpath or not os.path.exists(temp_fpath):
                    time.sleep(1)
                    for fname in os.listdir(DATA_INTERNAL_DIR):
                        if 'DOWNLOADED' not in fname and fname.endswith('.csv'):
                            temp_fpath = os.path.join(DATA_INTERNAL_DIR, fname)
                            break

                # Rename file
                os.rename(temp_fpath, downloaded_fpath)
                logging.error(f'Downloaded file for {c}: {downloaded_fpath}\n')
            except Exception as e: 
                logging.error(f'Error getting rGDP data for {c}: {e}')
                raise
    except Exception as e:
        logging.error(f'Error getting rGDP data: {e}')
        raise
    
    return True
        
def _get_countries(data):
    """
    Get countries from database.

    :param data: (pd.DataFrame) -> the data

    :return: (list<str>) -> the unique countries in the database
    """
    
    countries = list(data['country'].unique())

    return countries

def _get_country_codes(countries):
    """
    Translates plaintext country strings into URL country codes.

    :param countries: (list<str>) -> the countries in the database

    :return: (list<str>) -> the country codes
    """
    
    # Record counter-intuitive country codes used by FRED server
    country_to_code = {'Federated States of Micronesia': 'micronesia', 
                         'Democratic Republic of the Congo': 'dr%20congo',
                         'Republic of Serbia': 'serbia',
                         'United Republic of Tanzania': 'tanzania',
                         'United States of America': 'usa',
                         'Northern Mariana Islands': 'cnmi',
                         'S. Sudan': 'south%20sudan',
                         'Vietnam': 'viet%20nam',
                         'South Georgia and the Islands': None,
                         'Pitcairn Islands': None,
                         'Antarctica': None,
                         'French Polynesia': None,
                         'French Southern and Antarctic Lands': None,
                         'British Indian Ocean Territory': None
                         }
    try:
        # Map countries to codes
        country_codes = []
        for c in countries:
            try:
                if c == None: 
                    continue
                # Refer to map if code is counter-intuitive
                elif c in country_to_code:
                    code = country_to_code[c]
                    if code is not None:
                        country_codes.append(code)
                # Otherwise, build code here
                else:
                    code = c.lower()
                    if ' ' in c:
                        code = code.replace(' ', '%20')
                    country_codes.append(code)
            except Exception as e:
                logging.error(f'Error finding corresponding country code for {c}: {e}')
                raise
    except Exception as e:
        logging.error(f'Error getting country codes: {e}')
        raise
    return country_codes


def consolidate_rgdp_data(fname, keywords_hierarchy):
    """
    Consolidate country-specific rGDP data into a single dataframe.
    Save as .CSV to OUTPUT_DIR.

    :param filename: (str) -> filename of CSV

    :return: (pd.DataFrame) -> the dataframe with all the rGDP data
    """

    logging.log('Consolidating country-specific rGDP files into single .csv...')

    try:
        # Consolidate rGDP data into this dataframe
        rgdp_data = pd.DataFrame(columns=['Country', 'rGDP', 'Year'])
        # Restructure each country-level rGDP dataframe to match rgdp_data
        file_i = 1
        for filename in os.listdir(DATA_INTERNAL_DIR):
            try:
                if filename.endswith('.csv') and keywords_hierarchy[0] in filename: # Only include rGDP at constant price
                    logging.log(f'Processing {filename}... ({file_i})')
                    country = filename.split('_')[0]
                    
                    country_rgdp_data = pd.read_csv(os.path.join(DATA_INTERNAL_DIR, filename))

                    country_rgdp_data.rename(columns={country_rgdp_data.columns[1]: 'rGDP'}, inplace=True)
                    country_rgdp_data['Year'] = pd.to_datetime(country_rgdp_data['DATE']).dt.year
                    country_rgdp_data['Country'] = country

                    country_rgdp_data = country_rgdp_data[['Country', 'rGDP', 'Year']]
                    
                    rgdp_data = pd.concat([rgdp_data, country_rgdp_data], ignore_index=True)

                    file_i += 1
            except Exception as e:
                logging.error(f'Error adding rGDP data from {filename}: {e}')
                raise

        # Save the consolidated DataFrame to a new CSV file
        rgdp_data.to_csv(os.path.join(DATA_OUTPUT_DIR, fname), index=False)
    except Exception as e:
        logging.error(f'Error consolidating rGDP data: {e}')
        raise

    return rgdp_data    

    



    







