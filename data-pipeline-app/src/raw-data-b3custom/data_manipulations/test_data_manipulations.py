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
    df = dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    assert df["Country"][0] == "Germany"

def test_html_to_df_is_wrong():
    dh = DataHandle()
    df = dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    assert not (df["Country"][1] == "Germany")

def test_remove_last_n_rows():
    N_REMOVE = 1
    dh = DataHandle()
    df = dh.html_to_df(TEST_TABLE_HTML,table_class_name=TABLE_CLASS_NAME)
    df_removed = dh.remove_last_n_lines(df,n=N_REMOVE)
    assert (df.shape[0] - df_removed.shape[0]) == N_REMOVE

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