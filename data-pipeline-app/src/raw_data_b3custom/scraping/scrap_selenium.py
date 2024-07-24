import time
import logging
from typing import Type, Any

from selenium import webdriver
from selenium.webdriver import Chrome as WebDriverType
from selenium.webdriver import Edge as WebDriverType
from selenium.webdriver import Firefox as WebDriverType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


class ScrapSelenium():

    def __init__(self, 
                 browser:str = "Chrome",
                 max_webdriver_wait_til_secs:int = 10,
                 time_to_sleep_after_exec_secs:int = 2
                ) -> None:
        self.browser = browser
        self.max_webdriver_wait_til_secs = max_webdriver_wait_til_secs
        self.time_to_sleep_after_exec_secs = time_to_sleep_after_exec_secs

    def get_browser_driver(self,webdriver_to_get:str)->Type[WebDriverType]:
        
        logging.info(f"Trying to get {webdriver_to_get} webdriver...")
        driver: Type[WebDriverType]

        if (webdriver_to_get == "Chrome"):
            try:
                driver = webdriver.Chrome()
            except:
                logging.error(f"Unable to get {webdriver_to_get} webdriver.")
                raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
        
        elif (webdriver_to_get == "Edge"):
            try:
                driver = webdriver.Edge()
            except:
                logging.error(f"Unable to get {webdriver_to_get} webdriver.")
                raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
        
        elif (webdriver_to_get == "Firefox"):
            try:
                driver = webdriver.Firefox()
            except:
                logging.error(f"Unable to get {webdriver_to_get} webdriver.")
                raise Exception(f"Unable to get {webdriver_to_get} webdriver.")
            
        else:
            logging.error(f"Invalid webdriver option. Chose either 'Chrome', 'Edge' or 'Firefox'.")
            raise Exception(f"Invalid webdriver option. Chose either 'Chrome', 'Edge' or 'Firefox'.")

        logging.info(f"Found {webdriver_to_get} webdriver")
        return driver

    def get_to_html(
            self, url:str, xpath_elements:list[str])->str:

        driver = self.get_browser_driver(self.browser)
        webwait = WebDriverWait(driver, self.max_webdriver_wait_til_secs)
        driver.get(url)
        for xpath in xpath_elements:
            element = webwait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, xpath)
                )
            )
            element.click()
            driver.execute_script("arguments[0]", element)
            time.sleep(self.time_to_sleep_after_exec_secs)
        
        html=driver.page_source
        driver.quit()

        return html



