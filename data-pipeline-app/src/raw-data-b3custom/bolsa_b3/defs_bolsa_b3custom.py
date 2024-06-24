from .date_handling_b3custom import DateHandlingB3Custom

class BolsaB3ModelDefs():

    BASE_URL = "https://arquivos.b3.com.br/apinegocios/tickercsv/"
    DATE_FORMAT = "%Y-%m-%d"

    SCRAP_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
    XPATH_SELECT_TABLE_SIZE_VALUE_120 = '//*[@id="selectPage"]/option[4]'
    TABLE_CLASS_NAME = "table table-responsive-sm table-responsive-md"

class BolsaB3(BolsaB3ModelDefs):

    def __init__(self, strdate:str) -> None:
        self.strdate:str = strdate
        self.B3_url:str = None
        self.date_handle_b3 = DateHandlingB3Custom()
        pass

    def check_B3_date_format_is_valid(self,test_str_date:str)->bool:
        return self.date_handle_b3.check_date_format_is_valid(test_str_date,BolsaB3ModelDefs.DATE_FORMAT)

    def url_B3_parser(self)->str:
        return BolsaB3ModelDefs.BASE_URL+self.strdate
    
    def get_B3_url(self)->str:
        if (self.check_B3_date_format_is_valid(self.strdate)):
            if (self.date_handle_b3.check_if_workday(self.strdate)):
                self.B3_url = self.url_B3_parser()
                return self.B3_url
            else:
                raise Exception("Date is not a workday")    
        else:
            raise Exception("Date is not in a valid format (%Y-%m%d)")
        