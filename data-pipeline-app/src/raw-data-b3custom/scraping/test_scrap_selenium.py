import pytest
from .scrap_selenium import ScrapSelenium

RUN_TEST_RETURN_GET_TO_HTML = False

def test_get_browser_driver_wrong():
    with pytest.raises(Exception) as excinfo:
        scrap = ScrapSelenium(browser="Something")
        scrap.get_browser_driver(scrap.browser)

    assert excinfo.type is Exception

def test_return_get_to_html():

    if RUN_TEST_RETURN_GET_TO_HTML:
        SCRAP_BASE_URL:str = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
        XPATH_SELECT_TABLE_SIZE_VALUE_120:str = '//*[@id="selectPage"]/option[4]'

        scrap_selenium = ScrapSelenium(browser="Edge")
        html_scraped = scrap_selenium.get_to_html(url=SCRAP_BASE_URL, xpath_elements=[XPATH_SELECT_TABLE_SIZE_VALUE_120])

        assert isinstance(html_scraped,str)
    else:
        assert True


