from datetime import datetime

class DateHandlingB3Custom():

    def __init__(self) -> None:
        pass

    def check_date_format_is_valid(self, test_str_date:str, date_format:str)->bool:
        
        res = True
        # using try-except to check for truth value
        try:
            print("Valid Date for URL")
            res = bool(datetime.strptime(test_str_date, date_format))
        except ValueError:
            print("Invalid Date for URL")
            res = False
        
        return res
    
    def check_if_workday(self, test_str_date:str):
        test_date = datetime.strptime(test_str_date, "%Y-%m-%d").date()
        return test_date.weekday() in range(0,5)
