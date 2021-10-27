import typing as T
from time import sleep
import logging

import pandas as pd
import fmpsdk as fmp

FMP_API_RETRIES = 2
FMP_API_SLEEP_TIME = 70
FMP_API_KEY = "yourkeyhere"
FMP_DATE_FORMAT = "%Y-%m-%d"


def call_fmp(
        method: T.Callable,
        params: dict,
        retries: int = FMP_API_RETRIES,
        sleep_time: int = FMP_API_SLEEP_TIME
):
    """Calls an API method w/ params, if failed, sleeps for a given amount of time, tries again, and after a given number of retries still fails, raises an error. See: https://stackoverflow.com/a/23961254/10295948
    Parameters
    ----------
    method : `typing.Callable`
        API method
    params : `dict`
        Params to pass to method
    retries : `int`, optional
        Number of retries, by default `settings.FMP_API_RETRIES`
    sleep_time : `int`, optional
        Number of seconds to sleep after each retry, by default `settings.FMP_API_SLEEP_TIME`
    Raises
    ------
    `Exception`
        If request still fails after max retries
    """
    if(not params.get("apikey", None)):
        params["apikey"] = FMP_API_KEY

    for x in range(0, retries):
        try:
            res = method(**params)
            str_error = None
        except Exception as str_error:
            logging.error(f"FMP Request failed. {str(str_error)}")
            pass

        if str_error:
            if(x < retries):
                sleep(sleep_time)
            else:
                raise str_error
        else:
            break

    return res


RawPriceHistory = T.Dict[str, T.List[T.Dict]]


def get_stocks_history(
    symbols: T.List[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    show_progress: bool = True,
) -> RawPriceHistory:
    """Fetches data for a list of symbols from FMP API
    Parameters
    ----------
    symbols : `T.List[str]`
        List of symbols to get price action history for
    show_progress : `bool`, optional
        By default `True`
    start: `pd.Timestamp`, optional
        Start date for fetching data; overries `period` if passed, by default `None`
    end: `pd.Timestamp`, optional
        End date for fetching data, by default `pd.Timestamp.now(tz="UTC")`
    Returns
    -------
    `RawPriceHistory`
    """
    end_str = end.strftime(FMP_DATE_FORMAT)
    start_str = start.strftime(FMP_DATE_FORMAT)
    hist = {}
    for ix, symbol in enumerate(symbols):
        show_progress and logging.info(f"{ix}/{len(symbols)}")
        params = {
            "from_date": start_str,
            "to_date": end_str,
            "symbol": symbol}
        price_history: T.Dict = call_fmp(fmp.historical_price_full, params)
        if(not price_history):
            continue
        price_history: T.List[T.Dict] = price_history["historical"]
        hist[symbol] = price_history

    return hist