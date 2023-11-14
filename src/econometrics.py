from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
from collections import defaultdict
import time
import os


import database
from database import get_engine_neon, get_data



"""
Constants
"""
BASE_URL_RGDP = 'https://fred.stlouisfed.org/searchresults/?st=gdp&t={}&ob=sr&od=desc'
BASE_URL_UNEMPLOYMENT = 'https://fred.stlouisfed.org/searchresults/?st=unemployment%20rate&t={}&ob=sr&od=desc'
DOWNLOAD_DIR = '/Users/jadijosh/workspace/quaker/data/econometrics/'
RGDP_DIR = os.path.join(DOWNLOAD_DIR, 'rGDP')

def setup_driver(download_dir):
    """
    Set up Chrome driver.

    :param download_dir: (str) -> the destination directory for downloads

    :return: (selenium.webdriver) -> the web driver
    """
   
    print('Setting up Chrome driver...')

    # Set preferences for chrome driver
    chrome_options = Options()

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False, # Enable auto-download
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)

    return driver


def get_rgdp_data(country_codes, engine, driver):
    """
    Get rGDP data and write to database.

    :param country_codes: (list<str>) -> the codes for the target countries
    :param engine: (SQLAlchemy.engine) -> the database engine
    :param driver: (selenium.webdriver) -> the Chrome driver

    """

    print(f'Getting rGDP data for {len(country_codes)} countries...')

    keywords_hierarchy = ['Real GDP at Constant National Prices', 'Real Gross Domestic Product for',' Gross Domestic Product for', 'GDP']

    download_success = 0
    for c in country_codes:
        print(f'Downloading file for {c}...')
        
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
        
        print(best_result_link)
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
        downloaded_fname = f'{c}_{best_result_title}_DOWNLOADED.csv'
        downloaded_fpath = os.path.join(DOWNLOAD_DIR, downloaded_fname)

        # Rename file by waiting for it to appear in filesystem, then calling os.rename
        temp_fpath = None 
        while not temp_fpath or not os.path.exists(temp_fpath):
            time.sleep(1)
            for fname in os.listdir(DOWNLOAD_DIR):
                if 'DOWNLOADED' not in fname and fname.endswith('.csv'):
                    temp_fpath = os.path.join(DOWNLOAD_DIR, fname)
                    break

        # Rename file
        os.rename(temp_fpath, downloaded_fpath)
        download_success += 1
        print(f'Downloaded file for {c}: {downloaded_fpath}\n')
    
    print(f'Succesfully downloaded rGDP data for {download_success} / {len(country_codes)} countries')

    return True
        
def _get_countries(engine):
    """
    Get countries from database.

    :param engine: (SqlAlchemy.engine) -> the database engine

    :return: (list<str>) -> the countries in the database
    """
    
    query = '''
            SELECT DISTINCT "Country" from locations;
            '''
    countries = get_data(query, engine)['Country'].tolist()

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
    
    # Map countries to codes
    country_codes = []
    for c in countries:
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

    return country_codes


def consolidate_rgdp_data(filename):
    """
    Consolidate country-specific rGDP data into a single dataframe.
    Save as .CSV to RGDP_DIR.

    :param filename: (str) -> filename of CSV

    :return: (pd.DataFrame) -> the dataframe with all the rGDP data
    """

    # Consolidate rGDP data into this dataframe
    rgdp_data = pd.DataFrame(columns=['Country', 'rGDP', 'Year'])
   
    # Restructure each country-level rGDP dataframe to match rgdp_data
    for filename in os.listdir(RGDP_DIR):
        if filename.endswith('.csv'):
            country = filename.split('_')[0]
            
            country_rgdp_data = pd.read_csv(os.path.join(RGDP_DIR, filename))

            country_rgdp_data.rename(columns={country_rgdp_data.columns[1]: 'rGDP'}, inplace=True)
            country_rgdp_data['Year'] = pd.to_datetime(country_rgdp_data['DATE']).dt.year
            country_rgdp_data['Country'] = country

            country_rgdp_data = country_rgdp_data[['Country', 'rGDP', 'Year']]
            
            rgdp_data = pd.concat([rgdp_data, country_rgdp_data], ignore_index=True)

    # Save the consolidated DataFrame to a new CSV file
    rgdp_data.to_csv(os.path.join(RGDP_DIR, filename), index=False)

    return rgdp_data




if __name__ == '__main__':

    # Set up driver    
    driver = setup_driver(DOWNLOAD_DIR)

    # Get countries
    neon_engine = get_engine_neon()

    country_codes = _get_country_codes(_get_countries(neon_engine))

    get_rgdp_data(country_codes, None, driver)







