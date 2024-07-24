from bolsa_b3.defs_bolsa_b3custom import BolsaB3ModelDefs
from data_manipulations.data_manipulations import DataHandle
from scraping.scrap_selenium import ScrapSelenium

def scrap_raw_data_b3(path_to_save_parquet:str='raw-data-b3custom/parquets/'):

    b3 = BolsaB3ModelDefs()
    b3_xpaths = [b3.XPATH_SELECT_TABLE_SEGMENT, b3.XPATH_SELECT_TABLE_SIZE_VALUE_120]

    scrap_selenium = ScrapSelenium(browser="Edge")
    html_scraped = scrap_selenium.get_to_html(url=b3.SCRAP_BASE_URL, xpath_elements=b3_xpaths)

    dh = DataHandle()
    dh.get_and_treat_df(html=html_scraped, 
                        table_class_name=b3.TABLE_CLASS_NAME, 
                        n=2, 
                        rename_cols=b3.RENAME_DICT, 
                        qtde_col_name=b3.QTDE_COL_NAME)
    fullfilename = dh.save_df_to_named_parquet(b3.FILE_DESCRIPTION,b3.DT_FORMAT,filepath=path_to_save_parquet)
    print(fullfilename)

    df_read = dh.read_from_parquet(fullfilename)

    print(df_read)