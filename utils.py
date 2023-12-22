import re
import sys
import math
import time
from datetime import datetime, timedelta
from functools import wraps


def get_month(delta_month: int = 0, digit: int = 2) -> str:
    """
    This function is to provide the month with the option of adding or subtracting in the roling year.

    Args:
        - delta_month (int): month to add / substract from the current month
        - digit (int): amount of digits to be returend (digit=3 in May, resulot will be 005)

    Return:
        - month (str): actual month with resprect of the delta and the given digits
    """
    return str(
        list(range(1, 13))[(datetime.today().month - 1 + delta_month) % 12]
    ).zfill(digit)


def get_year(delta_month: int = 0) -> int:
    """
    This function is to provide the actual year taken into account a delta of month from today.

    Args:
        - delta_month (int): month to add / substract from the current date

    Return:
        - year (int): actual year with resprect of the delta of month provided
    """
    return datetime.today().year + math.floor(
        (datetime.today().month + delta_month - 1) / 12
    )


def get_last_weekday(delta: int = -1, date_format: str = "%Y-%m-%d"):
    """
    This function is to provide the date of the last weekday.

    Args:
        - delta (int): -1 by default, could be updated to earlier dates
        - date_format (str): definition of the output date format

    Return:
        - date (str): string date in the given date format

    """
    assert delta in range(-6, 1), (
        f"delta '{delta}' is not acceptable by function get_last_weekday. "
        + "Please use values between 0 & -6"
    )
    while True:
        _date = datetime.today() + timedelta(delta)
        if _date.weekday() < 5:
            return _date.strftime(date_format)
        delta -= 1


def get_month_weekday(delta_working_days: int = 0, date_format: str = "%Y-%m-%d"):
    """
    This Function will return the first weekday (moastly workingday).

    Args:
        - delta_working_days (int): delta of weekdays (e.g. 3 will return the 3rd weekday of the actual month).
        - date_format (str): definition of the output date format

    Return:
        - date (str): string date in the given date format
    """
    days = 0
    _working_days = 0
    assert delta_working_days >= 0, (
        f"delta '{delta_working_days}' is not acceptable by function get_month_weekday. "
        + "Please use values higher then 0"
    )
    while True:
        _date = datetime.today().replace(day=1) + timedelta(days)
        if _date.weekday() < 5:
            _working_days += 1
        if _working_days >= delta_working_days:
            return _date.strftime(date_format)
        days += 1


def timing(func):
    """
    This function is a wrapper function to get the eecution time of a function.
    Use it as decorator of a function like:

        @timing
        def some_function(some_var):
            print(some_var)

    Output looks like this:
        TIMING: function 'some_function' took: x.xxxx sec
    """

    @wraps(func)
    def timing_wrapper(*args, **kwargs):
        print(f"\tfunction {func.__name__} started")
        tstart = time.time()
        result = func(*args, **kwargs)
        tend = time.time()
        print("\tTIMING: function %r took %2.4f sec" % (func.__name__, tend - tstart))
        return result

    return timing_wrapper


def find_pattern(pattern: str, research: list, no_match_exit: bool = False) -> str:
    """
    This function finds a pattern (string) in a list of strings.
    Could be used to finde filenames with included date in a dynamic way.
    As wildcard following expression can be used: '(.*)', '*' or '%'.

    ATTENTION: first result will be returned!

    Example:
        - pattern: 'nominativeList_(.*)_2023.csv'
        - research: ['nominativeList_03_2023.csv', 'nominativeList_04_2023.csv']
        return: 'nominativeList_03_2023.csv'

    Args:
        - pattern (str): pattern to search for
        - research (list): list of strings to search in
        - no_match_exit (bool): exit's the scrip if no match found

    Return: string with the first match and 'no match' in case of no matching value
    """
    compiled = re.compile(pattern.replace("*", "(.*)").replace("%", "(.*)"))
    for s in research:
        result = compiled.search(s)
        if result is not None:
            return result.group().strip()
    else:
        if no_match_exit:
            sys.exit(f"no match found for pattern '{pattern}' in list\n{research}")
        else:
            return "no match"


if __name__ == "__main__":
    # example how to use get_year and get_month
    for i in range(-7, 7):
        print(str(get_year(delta_month=i)) + "-" + get_month(delta_month=i, digit=2))

    # example how to use timing decorator
    @timing
    def some_function():
        time.sleep(1)

    some_function()
