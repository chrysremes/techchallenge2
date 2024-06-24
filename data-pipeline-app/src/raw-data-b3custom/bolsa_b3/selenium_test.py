import time
import logging

import bs4
import pandas as pd
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

def get_browser_driver(webdriver_to_get = "Chrome"):

    logging.info(f"Trying to get {webdriver_to_get} webdriver...")
    if (webdriver_to_get == "Chrome"):
        try:
            driver = webdriver.Chrome()
        except:
            raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
    
    elif (webdriver_to_get == "Edge"):
        try:
            driver = webdriver.Edge()
        except:
            raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
    
    elif (webdriver_to_get == "Firefox"):
        try:
            driver = webdriver.Firefox()
        except:
            raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
        
    else:
        logging.error(f"Invalid webdriver option. Chose either 'Chrome', 'Edge' or 'Firefox'.")
        raise Exception(f"Invalid webdriver option. Chose either 'Chrome', 'Edge' or 'Firefox'.")

    logging.info(f"Found {webdriver_to_get} webdriver")
    return driver

def scrap_selenium_to_df():

    SCRAP_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
    XPATH_SELECT_TABLE_SIZE_VALUE_120 = '//*[@id="selectPage"]/option[4]'

    driver = get_browser_driver("Edge")
    webwait = WebDriverWait(driver, 10)
    driver.get(SCRAP_BASE_URL)
    element = webwait.until(
        EC.element_to_be_clickable(
            (By.XPATH, XPATH_SELECT_TABLE_SIZE_VALUE_120)
        )
    )
    element.click()
    driver.execute_script("arguments[0]", element)
    time.sleep(3)
    html=driver.page_source
    driver.quit()

    soup = bs4.BeautifulSoup(html,"html5lib")
    
    print("##############")
    print(soup)
    table = soup.find('table', attrs={'class':'table table-responsive-sm table-responsive-md'})
    print(table)
    print(pd.read_html(StringIO(str(table)))[0])

scrap_selenium_to_df()