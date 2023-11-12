from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By



"""
Constants
"""
BASE_URL_RGDP = 'https://fred.stlouisfed.org/searchresults/?st=gdp&t={}&ob=sr&od=desc'
BASE_URL_UNEMPLOYMENT = 'https://fred.stlouisfed.org/searchresults/?st=unemployment%20rate&t={}&ob=sr&od=desc'


def setup_driver(download_dir, exec_path):
   
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


def get_rgdp_data(countries, engine, driver):
    """
    Get rGDP data and write to database.

    :param countries: (list<str>) -> the target countries
    """

    print(f'Getting rGDP data for {countries}...')

    for c in countries:
        # Go to specific country data page
        url = BASE_URL_RGDP.format(c)
        driver.get(url)
        time.sleep(5)

        # Go to the country-specific rGDP page to get data
        search_results = driver.find_elements(By.CLASS_NAME, 'search-series-title-gtm')
        for item in search_results:
            if 'Real Gross Domestic Product' in item.get_attribute('aria-label'):
                item.click()
                time.sleep(5)

                # Click the download button
                download_button = driver.find_element(By.ID, 'download-button')
                download_button.click()
                time.sleep(2)

                # Download file
                csv_download_link = driver.find_element(By.ID, 'download-data-csv')
                driver.get(csv_download_link.get_attribute('href'))
                time.sleep(7)



if __name__ == '__main__':

    driver_exec_fpath = '/Users/jadijosh/workspace/chromedriver'
    download_dir = '/Users/jadijosh/workspace/quaker/data/econometrics/'
    driver = setup_driver(download_dir, driver_exec_fpath)

    countries = ['australia', 'canada', 'usa']

    get_rgdp_data(countries, None, driver)
