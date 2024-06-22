import time

import bs4
import pandas as pd
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By

def scrap_to_df_slnm():

    SCRAP_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"

    driver = webdriver.Edge()

    driver.get(SCRAP_BASE_URL)

    html=driver.page_source
    #element = driver.find_element(By.ID, 'table table-responsive-sm table-responsive-md')
    #element.send_keys('WebDriver')
    #element.submit()

    time.sleep(5)

    soup = bs4.BeautifulSoup(html,"html.parser")
    
    driver.quit()

    print("##############")
    print(soup)
    table = soup.find('table', attrs={'class':'table table-responsive-sm table-responsive-md'})
    print(table)
    print(pd.read_html(StringIO(str(table)))[0])

scrap_to_df_slnm()