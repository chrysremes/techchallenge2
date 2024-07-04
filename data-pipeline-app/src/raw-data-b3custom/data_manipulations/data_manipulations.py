import os
from datetime import datetime
import logging
from typing import Any

import bs4
import pandas as pd
from io import StringIO

class DataHandle():

    def __init__(self) -> None:
        self.df:pd.DataFrame = pd.DataFrame()

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

    def html_to_df(self, html:str, table_class_name:str)->None:
        table = self.html_to_soup(html, table_class_name)
        self.df = self.soup_to_df(table)

    def remove_last_n_lines(self, n:int)->None:
        logging.info(f"Removing last {n} rows from df")
        try:
            self.df.drop(self.df.tail(n).index,inplace=True)
        except:
            logging.error("Something wrong with converting soup table to df")
            raise Exception("Something wrong with converting soup table to df")

    def get_current_date(self,date_format:str="%Y-%m-%d")->datetime:
        return datetime.now().strftime(date_format)
    
    def remove_multi_index_header(self):
        self.df.columns = self.df.columns.droplevel(0)
        self.df.reset_index()
    
    def rename_df_columns(self,rename_dict:dict):
        self.df.rename(columns=rename_dict,inplace=True)

    def insert_data_column_to_df(self,date_to_insert:datetime,loc:int=0,col_name:str="Data"):
        self.df.insert(loc=loc,column=col_name,value=date_to_insert)

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

    def save_to_parquet(self, filename:str, filepath:str)->str:
        try:
            if not self.check_if_file_path_is_empty(filepath):
                logging.info(f"Non-empty filepath, check if path needs to be created")
                self.create_path_if_not_exists(filepath)
            else:
                logging.info(f"Filepath is empty, then use current path (pwd)")
            fullfilename = os.path.join(filepath, filename)
            logging.info(f"Writing {fullfilename} ...")
            print(f"Writing {fullfilename} ...")
            self.df.to_parquet(fullfilename)
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
    
    def get_and_treat_df(self, html:str, table_class_name:str, n=2, rename_cols:dict=None)->pd.DataFrame:
        self.html_to_df(html, table_class_name)
        self.remove_last_n_lines(n)
        self.remove_multi_index_header()
        if rename_cols is not None:
            self.rename_df_columns(rename_cols)
        self.insert_data_column_to_df(date_to_insert=self.get_current_date())
        return self.df
    
    def save_df_to_named_parquet(self, file_prefix:str, date_format:str, filepath:str="")->str:
        filename = self.create_parquet_filename(file_prefix, date_format)
        return self.save_to_parquet(filename,filepath)
