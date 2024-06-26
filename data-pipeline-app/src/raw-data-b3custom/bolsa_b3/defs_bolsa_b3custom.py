class BolsaB3ModelDefs():

    SCRAP_BASE_URL:str = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
    XPATH_SELECT_TABLE_SIZE_VALUE_120:str = '//*[@id="selectPage"]/option[4]'
    XPATH_SELECT_TABLE_SEGMENT:str = '//*[@id="segment"]/option[2]'
    TABLE_CLASS_NAME:str = "table table-responsive-sm table-responsive-md"

    FILE_DESCRIPTION:str = "DataB3"
    DT_FORMAT:str = "%Y%m%d"

    TIME_TO_SLEEP:int = 2
        