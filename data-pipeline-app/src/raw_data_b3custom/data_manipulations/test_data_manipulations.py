import pytest
from .data_manipulations import DataHandle
import pandas as pd
from io import StringIO
from datetime import datetime

TABLE_CLASS_NAME = "table_test"
TEST_TABLE_HTML = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>0123</title>
</head>
<body>
    <table class="{TABLE_CLASS_NAME}">
        <tr>
            <th>Contact</th>
            <th>Country</th>
        </tr>
        <tr>
            <td>Maria Anders</td>
            <td>Germany</td>
        </tr>
        <tr>
            <td>Francisco Chang</td>
            <td>Mexico</td>
        </tr>
    </table>
</body>
</html>
"""

RENAME_DICT = {
        "Código" : "Codigo",
        "Ação" : "Acao",
        "Qtde. Teórica" : "Qtde",
        "Part. (%)" : "Setor_Part",
        "Part. (%)Acum." : "Setor_Part_Ac"
    }

def test_html_to_soup_is_ok():
    dh = DataHandle()
    table = dh.html_to_soup(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    df = pd.read_html(StringIO(str(table)))[0]
    assert df["Country"][0] == "Germany"

def test_html_to_soup_is_wrong():
    dh = DataHandle()
    table = dh.html_to_soup(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    df = pd.read_html(StringIO(str(table)))[0]
    assert not (df["Country"][1] == "Germany")

def test_html_to_df_is_ok():
    dh = DataHandle()
    dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    assert dh.df["Country"][0] == "Germany"

def test_html_to_df_is_wrong():
    dh = DataHandle()
    dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    assert not (dh.df["Country"][1] == "Germany")

def test_remove_last_n_rows():
    N_REMOVE = 1
    dh = DataHandle()
    dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    df0 = dh.df.copy()
    dh.remove_last_n_lines(n=N_REMOVE)
    df_removed = dh.df
    assert (df0.shape[0] - df_removed.shape[0]) == N_REMOVE

def test_rename_df_columns():
    TEST_FILE = "Test_Data.parquet"
    dh = DataHandle()
    df0 = dh.read_from_parquet(TEST_FILE)
    df0.columns = df0.columns.droplevel(0)
    df0.reset_index()
    dh.df = df0.copy()
    dh.rename_df_columns(RENAME_DICT)
    assert ("Codigo" in dh.df.columns and "Código" not in dh.df.columns)

def test_create_parquet_filename_is_ok():
    dh = DataHandle()
    PREFIX = "Prefix"
    DTFORMAT = "%Y%m%d"
    dtnow = datetime.now().strftime(DTFORMAT)

    filename = dh.create_parquet_filename(PREFIX,DTFORMAT)
    expected = PREFIX+"_"+dtnow+".parquet"
    assert filename == expected

def test_check_if_file_path_is_empty_is_ok():
    dh = DataHandle()
    test_value = dh.check_if_file_path_is_empty("")
    expected = True
    assert (test_value == expected)

def test_check_if_file_path_is_empty_is_wrong():
    dh = DataHandle()
    test_value = dh.check_if_file_path_is_empty("aa")
    expected = False
    assert (test_value == expected)

# OTHER TO DO's: 
# test other right/wrong possible cases with mark.parametrize
# test if parquet has been written (modified) with hashlib 
# test if parquet has been read correctly