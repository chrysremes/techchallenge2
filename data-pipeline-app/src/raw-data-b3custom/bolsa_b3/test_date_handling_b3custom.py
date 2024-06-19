import pytest
from .date_handling_b3custom import DateHandlingB3Custom

# from datetime import datetime
# from .web_scrapping import EmbrapaCollect

# class DateHandlingB3Custom():

#     def __init__(self) -> None:
#         pass

#     def check_date_format_is_valid(self, test_str_date:str, date_format:str)->bool:
        
#         res = True
#         # using try-except to check for truth value
#         try:
#             print("Valid Date for URL")
#             res = bool(datetime.strptime(test_str_date, date_format))
#         except ValueError:
#             print("Invalid Date for URL")
#             res = False
        
#         return res
    
#     def check_if_workday(self, test_str_date:str):
#         test_date = datetime.strptime(test_str_date, "%Y-%m-%d").date()
#         return test_date.weekday() in range(0,5)

@pytest.mark.parametrize("test_str_date, date_format, expected_result", [
    ("2000-01-01", "%Y-%m-%d", True),
    ("2000-14-01", "%Y-%m-%d", False),
    ("2000-14-01", "%Y-%d-%m", True),
])
def test_check_date_format_is_valid(test_str_date:str,date_format:str, expected_result:bool):
    date_handle = DateHandlingB3Custom()
    result = date_handle.check_date_format_is_valid(test_str_date,date_format)
    assert result == expected_result


@pytest.mark.parametrize("test_str_date, expected_result", [
    ("2024-06-10", True),
    ("2024-06-11", True),
    ("2024-06-12", True),
    ("2024-06-13", True),
    ("2024-06-14", True),
    ("2024-06-15", False),
    ("2024-06-16", False),
    ("2024-06-17", True),
])
def test_check_if_workday(test_str_date:str, expected_result:bool):
    date_handle = DateHandlingB3Custom()
    result = date_handle.check_if_workday(test_str_date)
    assert result == expected_result

