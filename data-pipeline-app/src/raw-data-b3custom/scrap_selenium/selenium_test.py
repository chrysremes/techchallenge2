import time
import os
from datetime import datetime
import logging

import bs4
import pandas as pd
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SCRAP_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
XPATH_SELECT_TABLE_SIZE_VALUE_120 = '//*[@id="selectPage"]/option[4]'
TABLE_CLASS_NAME = "table table-responsive-sm table-responsive-md"

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

def scrap_selenium_to_html(browser,url,xpath_element):

    driver = get_browser_driver(browser)
    webwait = WebDriverWait(driver, 10)
    driver.get(url)
    element = webwait.until(
        EC.element_to_be_clickable(
            (By.XPATH, xpath_element)
        )
    )
    element.click()
    driver.execute_script("arguments[0]", element)
    time.sleep(2)
    html=driver.page_source
    driver.quit()

    return html

def html_to_pd_bs4(html,table_class_name):
    soup = bs4.BeautifulSoup(html,"html5lib")
    table = soup.find('table', attrs={'class':table_class_name})
    return (pd.read_html(StringIO(str(table)))[0])

def remove_last_two_lines(df:pd.DataFrame):
    return df.drop(df.tail(2).index)

def create_filename():
    FILE_DESCRIPTION = "DataB3"
    DT_FORMAT = "%Y%m%d"
    FILE_EXT = ".parquet"
    dtnow = datetime.now().strftime(DT_FORMAT)
    print("dtnow="+dtnow)
    filename = FILE_DESCRIPTION+"_"+dtnow+FILE_EXT
    return filename

def save_to_parquet(df:pd.DataFrame,filename,filepath):

    if not (filepath == ""):
        if not os.path.exists("./"+filepath):
            logging.info(f"Creating new {filepath} directory.")
            os.makedirs("./"+filepath)
        else:
            logging.info(f"Dir {filepath} already exists.")
    fullfilename = os.path.join(filepath, filename)
    logging.info(f"Writing {fullfilename} ...")
    print(f"Writing {fullfilename} ...")
    df.to_parquet(fullfilename)
    logging.info(f"Saved {fullfilename} file")
    print(f"Saved {fullfilename} file")
    return fullfilename

def read_from_parquet(fullfilename):
    try:
        logging.info("Trying to read parquet with pyarrow")
        df_read = pd.read_parquet(fullfilename, engine='pyarrow')
    except:
        try:
            logging.info("Trying to read parquet with fastparquet")
            df_read = pd.read_parquet(fullfilename, engine='fastparquet')
        except:
            logging.error("Unable to read parquet file")
            raise Exception("Unable to read parquet file")
    logging.info("Parquet file read successfully!")
    return df_read

html = scrap_selenium_to_html("Edge",SCRAP_BASE_URL,XPATH_SELECT_TABLE_SIZE_VALUE_120)
df = html_to_pd_bs4(html,TABLE_CLASS_NAME)
df = remove_last_two_lines(df)
filename = create_filename()
print(filename)
fullfilename = save_to_parquet(df,filename,filepath="saved_parquet")

print(df)
print(read_from_parquet(fullfilename))