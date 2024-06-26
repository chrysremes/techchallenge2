import os
from datetime import datetime
import logging
from typing import Any

import bs4
import pandas as pd
from io import StringIO

class DataHandle():

    def __init__(self) -> None:
        pass

    def html_to_soup(self, html:str,table_class_name:str)->Any:
        logging.info("Finding table within scraped html source")
        try:
            soup = bs4.BeautifulSoup(html,"html5lib")
            return soup.find('table', attrs={'class':table_class_name})
        except:
            logging.error("Something wrong with getting Table with bs4 from html")
            raise Exception("Something wrong with getting Table with bs4 from html")
    
    def soup_to_df(self, table:Any)->pd.DataFrame:
        logging.info("Converting bs4 table to df")
        try:
            return (pd.read_html(StringIO(str(table)))[0])
        except:
            logging.error("Something wrong with converting soup table to df")
            raise Exception("Something wrong with converting soup table to df")

    def html_to_df(self, html:str, table_class_name:str)->pd.DataFrame:
        table = self.html_to_soup(html, table_class_name)
        return self.soup_to_df(table)

    def remove_last_n_lines(self, df:pd.DataFrame, n:int)->pd.DataFrame:
        logging.info(f"Removing last {n} rows from df")
        try:
            return df.drop(df.tail(n).index)
        except:
            logging.error("Something wrong with converting soup table to df")
            raise Exception("Something wrong with converting soup table to df")

    def create_parquet_filename(self, file_prefix:str, date_format:str)->str:
        try:
            dtnow = datetime.now().strftime(date_format)
            filename = file_prefix+"_"+dtnow+".parquet"
            logging.info(f"Creating filename='{filename}'")
        except:
            logging.error("Error when creating filename string")
            raise Exception("Error when creating filename string")
        return filename

    def check_if_file_path_is_empty(self, filepath:str)->bool:
        return (filepath == "")
    
    def create_path_if_not_exists(self, filepath:str)->None:
        if not os.path.exists("./"+filepath):
            logging.info(f"Creating new {filepath} directory.")
            os.makedirs("./"+filepath)
        else:
            logging.info(f"Dir {filepath} already exists.")

    def save_to_parquet(self, df:pd.DataFrame, filename:str, filepath:str)->str:
        try:
            if not self.check_if_file_path_is_empty(filepath):
                logging.info(f"Non-empty filepath, check if path needs to be created")
                self.create_path_if_not_exists(filepath)
            else:
                logging.info(f"Filepath is empty, then use current path (pwd)")
            fullfilename = os.path.join(filepath, filename)
            logging.info(f"Writing {fullfilename} ...")
            print(f"Writing {fullfilename} ...")
            df.to_parquet(fullfilename)
            logging.info(f"Saved {fullfilename} file")
            print(f"Saved {fullfilename} file")
        except:
            logging.error("Error when saving to parquet")
            raise Exception("Error when saving to parquet")
        return fullfilename

    def read_from_parquet(self, fullfilename:str)->pd.DataFrame:
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
    
    def get_df_and_remove_n_last_lines(self, html:str, table_class_name:str, n=2):
        df = self.html_to_df(html, table_class_name)
        return self.remove_last_n_lines(df,n)
    
    def save_df_to_named_parquet(self, df:pd.DataFrame, file_prefix:str, date_format:str, filepath:str="")->str:
        filename = self.create_parquet_filename(file_prefix, date_format)
        return self.save_to_parquet(df,filename,filepath)
