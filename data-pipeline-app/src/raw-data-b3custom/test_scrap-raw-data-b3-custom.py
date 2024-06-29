import pytest

from bolsa_b3.defs_bolsa_b3custom import BolsaB3ModelDefs
from data_manipulations.data_manipulations import DataHandle
from scraping.scrap_selenium import ScrapSelenium

# def test_main_saved_and_read_ok():

#     PATH_SAVE_PARQUET = ""
#     b3 = BolsaB3ModelDefs()
#     b3_xpaths = [b3.XPATH_SELECT_TABLE_SEGMENT, b3.XPATH_SELECT_TABLE_SIZE_VALUE_120]

#     scrap_selenium = ScrapSelenium(browser="Edge")
#     html_scraped = scrap_selenium.get_to_html(url=b3.SCRAP_BASE_URL, xpath_elements=b3_xpaths)

#     dh = DataHandle()
#     df = dh.get_df_and_remove_n_last_lines(html=html_scraped, table_class_name=b3.TABLE_CLASS_NAME, n=2)
#     fullfilename = dh.save_df_to_named_parquet(df,b3.FILE_DESCRIPTION,b3.DT_FORMAT,filepath=PATH_SAVE_PARQUET)

#     df_read = dh.read_from_parquet(fullfilename)

#     assert df == df_read

def test_main_df_content():

    PATH_SAVE_PARQUET = ""
    b3 = BolsaB3ModelDefs()
    b3_xpaths = [b3.XPATH_SELECT_TABLE_SEGMENT, b3.XPATH_SELECT_TABLE_SIZE_VALUE_120]

    scrap_selenium = ScrapSelenium(browser="Edge")
    html_scraped = scrap_selenium.get_to_html(url=b3.SCRAP_BASE_URL, xpath_elements=b3_xpaths)

    dh = DataHandle()
    df_not_removed = dh.get_and_treat_df(html=html_scraped, table_class_name=b3.TABLE_CLASS_NAME, n=0)

    df = dh.get_and_treat_df(html=html_scraped, table_class_name=b3.TABLE_CLASS_NAME, n=2)
    
    fullfilename = dh.save_df_to_named_parquet(b3.FILE_DESCRIPTION,b3.DT_FORMAT,filepath=PATH_SAVE_PARQUET)

    df_read = dh.read_from_parquet(fullfilename)

    check_col = ("Setor" in df_read.columns)
    check_weg_ok = (df['Código'].eq('WEGE3')).any()
    check_weg_wrong = (df['Código'].eq('WEGE2')).any()
    check_removed_1 = (df['Setor'].eq('Quantidade Teórica Total')).any()
    check_removed_2 = (df['Setor'].eq('Redutor')).any()
    check_not_removed_1 = (df_not_removed['Setor'].eq('Quantidade Teórica Total')).any()
    check_not_removed_2 = (df_not_removed['Setor'].eq('Redutor')).any()
    check_df_equals = df.equals(df_read)

    all_checks = [0, 0, 0, 0, 0, 0, 0, 0]
    if check_col: all_checks[0] = 1
    if check_weg_ok.item(): all_checks[1] = 1
    if not check_weg_wrong.item(): all_checks[2] = 1
    if not check_removed_1.item(): all_checks[3] = 1
    if not check_removed_2.item(): all_checks[4] = 1
    if check_not_removed_1.item(): all_checks[5] = 1
    if check_not_removed_2.item(): all_checks[6] = 1
    if check_df_equals: all_checks[7] = 1
    
    assert all_checks == [1, 1, 1, 1, 1, 1, 1, 1]