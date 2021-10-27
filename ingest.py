"""Set of functions used to ingest FMP data to a Zipline bundle. See: https://github.com/quantopian/zipline/blob/master/zipline/data/bundles/quandl.py"""


import typing as T
from logbook import Logger

import pandas as pd
import numpy as np
from zipline.utils.calendars import TradingCalendar

from fmp import get_stocks_history

log = Logger(__name__)

RawPriceHistory = T.Dict[str, T.List[T.Dict]]


def gen_asset_metadata(
    data: pd.DataFrame, show_progress: bool, exchange: str
) -> pd.DataFrame:
    if show_progress:
        log.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": [np.min, np.max]})
    data.reset_index(inplace=True)
    data["start_date"] = data.date.amin
    data["end_date"] = data.date.amax
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = exchange
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data


def _format_price_history(
    price_history: pd.DataFrame
) -> pd.DataFrame:
    # use adjClose
    price_history["close"] = price_history["adjClose"]

    # remove extra cols
    cols_to_keep = ["open", "high", "low", "close", "volume"]
    cols_to_remove = list(set(price_history.columns) - set(cols_to_keep))
    price_history.drop(columns=cols_to_remove, inplace=True)

    # some assets in FMP don't have volume data
    if("volume" not in price_history.columns):
        price_history["volume"] = 0

    return price_history


def _set_dates_for_calendar(
        price_history: pd.DataFrame,
        calendar: TradingCalendar,
        start: pd.Timestamp,
        end: pd.Timestamp,
        symbol_map: pd.Series
) -> pd.DataFrame:
    """Sets price history dataframe date column vals based on zipline Trading Calendar and inserts `NaN` for missing dates to avoid `AssertionError` of missing sessions from zipline. See: https://github.com/quantopian/zipline/issues/2195#issuecomment-392933283"""
    # get bundle calendar sessions
    sessions = calendar.sessions_in_range(start, end)

    # set multiindex to date and symbol
    price_history.set_index(["date", "symbol"], inplace=True, drop=True)
    price_history.sort_index(inplace=True, level="date")

    # FMP sometimes returns a date twice for the same symbol
    price_history = price_history[
        ~price_history.index.duplicated(keep='first')]

    indices = [(date, symbol) for date in sessions for symbol in symbol_map]
    price_history = price_history.reindex(indices)

    return price_history


def convert_price_to_df(
        raw_price: RawPriceHistory,
) -> pd.DataFrame:
    all_history = pd.DataFrame()

    symbols = raw_price.keys()
    for symbol in symbols:
        price_history = raw_price[symbol]
        price_history_df = pd.DataFrame(price_history)
        price_history_df["symbol"] = symbol
        price_history_df["date"] = pd.to_datetime(
            price_history_df.date).dt.tz_localize("UTC")
        all_history = all_history.append(price_history_df)
    return all_history


def parse_pricing_and_vol(
    data: pd.DataFrame, calendar: TradingCalendar, start: pd.Timestamp, end: pd.Timestamp, symbol_map: pd.Series
):
    data = _set_dates_for_calendar(data, calendar, start, end, symbol_map)
    data = _format_price_history(data)
    for asset_id, symbol in symbol_map.items():
        yield asset_id, data.xs(symbol, level="symbol")


def ingest_fmp(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    exchange = ["US Equities", "NYSE", "US"]

    # set custom period here #
    period = 100
    # set custom list of stocks here #
    stocks = ["QQQ", "SPY"]

    # fetch and process price data
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=period)
    raw_data: RawPriceHistory = get_stocks_history(
        stocks, start=start, end=end)
    raw_data_df: pd.DataFrame = convert_price_to_df(raw_data)

    # write assets and exchanges
    asset_metadata = gen_asset_metadata(
        raw_data_df[["symbol", "date"]], show_progress, exchange[0])
    exchanges = pd.DataFrame(
        data=[exchange],
        columns=["exchange", "canonical_name", "country_code"],
    )
    asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)

    # write price data
    symbol_map: pd.Series = asset_metadata.symbol
    parsed_price = parse_pricing_and_vol(
        raw_data_df, calendar, start, end, symbol_map)
    daily_bar_writer.write(
        parsed_price,
        show_progress=show_progress,
    )

    # write adjusments
    splits_df = pd.DataFrame(columns=["sid", "ratio", "effective_date"])
    dividends_df = pd.DataFrame(
        columns=["sid", "record_date", "declared_date", "pay_date", "amount", "ex_date"])
    adjustment_writer.write(
        splits=splits_df,
        dividends=dividends_df,
    )
