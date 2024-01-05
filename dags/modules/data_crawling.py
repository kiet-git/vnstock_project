#Data Crawling

#Install and import packages
from vnstock import *
# from vnstock import (listing_companies, ticker_price_volatility, company_insider_deals, company_events, company_news, stock_historical_data, stock_intraday_data, stock_evaluation, general_rating, biz_model_rating, biz_operation_rating, financial_health_rating, valuation_rating, industry_financial_health, funds_listing, fund_details, company_overview, company_profile, company_large_shareholders, company_fundamental_ratio, company_subsidiaries_listing, company_officers, financial_ratio, financial_report)
from datetime import datetime
import pandas as pd
from requests.exceptions import ConnectTimeout
import time
import re
import logging
import os


# Crawled data 
'''
Data to be crawled:
- Daily
    - Công ty (Companies):
    
        - Danh sách công ty (Company listing)
        - Mức biến động giá cổ phiếu (Ticker price volatility)
        - Thông tin giao dịch nội bộ (Company insider deals)
        - Thông tin sự kiện quyền (Company events)
        - Tin tức công ty (Company news)
        
        - Giá cổ phiếu (Stock history)
        - Dữ liệu khớp lệnh trong ngày giao dịch (Stock intraday)
        - Định giá cổ phiếu (Stock evaluation)
        - Đánh giá cổ phiếu (Stock rating)
    - Quỹ (Funds):
        - Danh sách quỹ (Funds listing)
        - Các mã quỹ nắm giữ (Top holding list details)
        - Ngành mà quỹ đang đầu tư (Industry holding list details)
        - Báo cáo NAV (Nav report)
        
        - Tỉ trọng tài sản nắm giữ (Asset holding list)
        
- Quarterly:
    - Thông tin tổng quan (Company overview)
    - Hồ sơ công ty (Company profile)
    - Danh sách cổ đông (Company large shareholders)
    - Các chỉ số tài chính cơ bản (Company fundamental ratio)
    - Danh sách công ty con, công ty liên kết (Company subsidiaries listing)
    - Ban lãnh đạo công ty (Company officers)
    - Chỉ số tài chính cơ bản (Financial ratio)

    - Báo cáo kinh doanh (Income statement)
    - Bảng cân đối kế toán (Balance sheet)
    - Báo cáo lưu chuyển tiền tệ (Cash flow)
'''

#------------------------------------------------------------------------

# Daily

# Companies

# Danh sách công ty (Company listing)
def capture_company_listing():
    try:
        df = listing_companies(live=True)
    except Exception as e:
        error_message = f"Error capturing company listing data: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Mức biến động giá cổ phiếu (Ticker price volatility)
def capture_ticker_volatility(*args):
    symbol = args[0]
    try:
        df = ticker_price_volatility(symbol=symbol)
    except Exception as e:
        error_message = f"Error capturing ticker price volatility data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df


# Thông tin giao dịch nội bộ (Company insider deals)
def capture_insider_deals(*args, page_size=30):
    symbol = args[0]
    capture_date = args[1]
    date_string = capture_date.strftime("%Y-%m-%d")

    try:
        df = company_insider_deals (
            symbol=symbol, 
            page_size=page_size, 
            page=0)
    except Exception as e:
        error_message = f"Error capturing company insider deals data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df[df['dealAnnounceDate'] == date_string]

# Thông tin sự kiện quyền (Company events)
def capture_company_event(*args, page_size=10):
    symbol = args[0]
    capture_date = args[1]
    try:
        df = company_events(
            symbol=symbol, 
            page_size=page_size, 
            page=0)

    except Exception as e:
        error_message = f"Error capturing company events data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    
    df = df.apply(pd.to_numeric, errors='ignore')
    df["exerDateFormatted"] = pd.to_datetime(df['exerDate'], format="%Y-%m-%d %H:%M:%S")
    return df[df['exerDateFormatted'] == capture_date]

# Tin tức công ty (Company news)
def capture_company_news(*args, page_size=10):
    symbol = args[0]
    capture_date = args[1]

    try:
        df = company_news(
            symbol=symbol, 
            page_size=page_size, 
            page=0)

    except Exception as e:
        error_message = f"Error capturing company news for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    
    df = df.apply(pd.to_numeric, errors='ignore')
    df["publishDateFormatted"] = pd.to_datetime(df['publishDate'], format="%Y-%m-%d %H:%M:%S")
    return df[df['publishDateFormatted'] == capture_date]


# Giá cổ phiếu (Stock history) 
def capture_stock_history(*args):
    symbol = args[0]
    capture_date = args[1]
    date_string = capture_date.strftime("%Y-%m-%d")

    try:
        df = stock_historical_data(
            symbol=symbol,
            start_date=date_string,
            end_date=date_string,
            resolution='1D',
            type="stock",
            beautify=True,
            decor=False,
            source='DNSE'
        )
    except Exception as e:
        error_message = f"Error capturing historical data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    
    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Dữ liệu khớp lệnh trong ngày giao dịch (Stock intraday)
def capture_stock_intraday(*args, page_size=1000):
    symbol = args[0]
    
    try:
        df = stock_intraday_data(
            symbol=symbol,                 
            page_size=page_size, 
            investor_segment=True)
    except Exception as e:
        error_message = f"Error capturing stock intraday data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Định giá cổ phiếu (Stock evaluation)
def capture_stock_evaluation(*args):
    symbol = args[0]
    capture_date = args[1]
    date_string = capture_date.strftime("%Y-%m-%d")

    try:
        df = stock_evaluation(
            symbol=symbol,
            period=1, 
            time_window='D')
    except Exception as e:
        error_message = f"Error capturing evaluation data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df[df['fromDate'] == date_string]


# Đánh giá cổ phiếu (Stock rating)
def capture_stock_rating(*args):
    symbol = args[0]

    try:
        df_general = general_rating(symbol)
        df_biz_model = biz_model_rating(symbol)
        df_biz_operation = biz_operation_rating(symbol)
        df_financial_health = financial_health_rating(symbol)
        df_valuation = valuation_rating(symbol)
        df_industry_health = industry_financial_health(symbol)

        dfs = [df_general, df_biz_model, df_biz_operation, df_financial_health, df_valuation, df_industry_health]

        df_merged = pd.concat([df.set_index('ticker') for df in dfs], axis=1, join='outer').reset_index()

        df_merged = df_merged.loc[:,~df_merged.columns.duplicated()]

    except Exception as e:
        error_message = f"Error capturing stock rating data for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df_merged = df_merged.apply(pd.to_numeric, errors='ignore')
    return df_merged

# Crawl all daily companies data and store
def retry_request(func, *args, max_retries=3, retry_delay=20):
    for _ in range(max_retries):
        try:
            return func(*args)
        except ConnectTimeout as e:
            logger.error(f"Connection timeout: {e}")
            logger.error(f"Retrying in {retry_delay} seconds...")
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    logger.error("Max retries exceeded. Skipping.")
    print("Max retries exceeded. Skipping.")
    return None

def process_symbols(symbols, capture_function, *args):
    data_dict = {}

    for i, symbol in enumerate(symbols, start=1):
        data = retry_request(capture_function, symbol, *args)
        if not data.empty:
            data.columns = [convert_column_name(column) for column in data.columns]
            data_dict[symbol] = data
        print(f"Processed {i}/{len(symbols)} symbols of {capture_function.__name__}")

    return data_dict

def concat_if_not_empty(data_dict):
    df_list = [df.loc[:,~df.columns.duplicated()].reset_index(drop=True) for df in data_dict.values() if not df.empty]
    return pd.concat(df_list, keys=data_dict.keys(), names=['Symbol']) if df_list else None

def get_number_of_rows(data_dict):
    return len(data_dict) if data_dict is not None else 0

def convert_column_name(column_name):
    spaced_name = re.sub(r'(?<=[a-z])([A-Z])', r' \1', column_name)
    title_case_name = spaced_name.title().replace("_", " ")
    return title_case_name  

def export_data_to_excel(data_dict, listing_df, list_name, file_path, capture_mode):
    
    excel = pd.ExcelWriter(file_path, mode='a', if_sheet_exists='replace') if os.path.exists(file_path) else pd.ExcelWriter(file_path, mode='w')

    with excel as writer:
        summary_df = pd.DataFrame({
            'Sheet Name': [convert_column_name(sheet_name) for sheet_name in data_dict.keys()],
            'Number of Rows': [get_number_of_rows(data) for data in data_dict.values()]
        })

        company_listing_summary = pd.DataFrame({
            'Sheet Name': [f'{list_name} Listing'],
            'Number of Rows': [get_number_of_rows(listing_df)]
        })
        summary_df = pd.concat([company_listing_summary, summary_df], ignore_index=True)

        summary_df.to_excel(writer, sheet_name=f'{capture_mode} {list_name} Info Summary', index=False)

        listing_df.columns = [convert_column_name(column) for column in listing_df.columns]
        listing_df.to_excel(writer, sheet_name=f'{list_name} Listing', index=False)
        

        for function_name, data in data_dict.items():
            df = concat_if_not_empty(data)
            if df is not None:
                if 'Symbol' not in df.columns:
                    df = df.reset_index() 
                    df = df.drop('level_1', axis=1)
                df = df.set_index('Symbol')
                if 'Ticker' in df.columns:
                    df = df.drop('Ticker', axis=1)

                df.to_excel(writer, sheet_name=convert_column_name(function_name))

def capture_all_stock_data(capture_date, file_path, limit=None):
    logger.info("Capture all daily company data started.")
    company_listing_df = capture_company_listing()
    if limit is not None:
        company_symbols = company_listing_df["ticker"][:limit]
    else:
        company_symbols = company_listing_df["ticker"]

    functions_to_capture = [
        capture_ticker_volatility,
        capture_insider_deals,
        capture_company_event,
        capture_company_news,
        capture_stock_history,
        capture_stock_intraday,
        capture_stock_evaluation,
        capture_stock_rating,
    ]

    data_dict = {f.__name__: process_symbols(company_symbols, f, capture_date) for f in functions_to_capture}

    export_data_to_excel(data_dict, company_listing_df, 'Company', file_path, "Daily")
    
    logger.info("Capture all daily company data ended.")
    return company_listing_df, *data_dict.values()


# Funds

# Danh sách quỹ (Funds listing)
def capture_fund_listing():
    try:
        df = funds_listing(
            lang='en', 
            fund_type="",
            mode="full",
            decor=False
        )
    except Exception as e:
        error_message = f"Error capturing fund listing data: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Fund Details
'''
- Các mã quỹ nắm giữ (Top holding list details)
- Ngành mà quỹ đang đầu tư (Industry holding list details)
- Báo cáo NAV (Nav report)
- Tỉ trọng tài sản nắm giữ (Asset holding list)
'''
FUND_DETAILS_CATE = ["top_holding_list", "industry_holding_list", "nav_report", "asset_holding_list"]
def capture_fund_details(*args):
    symbol = args[0]
    type = args[1]
    if len(args) == 3:
        capture_date = args[2]
        date_string = capture_date.strftime("%Y-%m-%d")

    try:
        if type not in FUND_DETAILS_CATE:
            raise ValueError(f"Invalid fund details type '{type}'. Type must be one of {FUND_DETAILS_CATE}")
        
        df = fund_details(
            symbol=symbol, 
            type=type
        )
    except Exception as e:
        error_message = f"Error capturing fund details data of {type} for {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    if 'Ngày' in df.columns:
        return df[df['Ngày'] == date_string]

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Crawl all funds daily data and store
def capture_all_fund_data(capture_date, file_path, limit=None):
    logger.info("Capture all daily fund data started.")
    fund_listing_df = capture_fund_listing()
    if limit is not None:
        fund_symbols = fund_listing_df["shortName"][:limit]
    else:
        fund_symbols = fund_listing_df["shortName"]

    data_dict = {s: process_symbols(fund_symbols, capture_fund_details, s, capture_date) for s in FUND_DETAILS_CATE}

    export_data_to_excel(data_dict, fund_listing_df, 'Fund', file_path, "Daily")
    
    logger.info("Capture all daily fund data ended.")
    return fund_listing_df, *data_dict.values() 
    
# Crawl all daily data and store
def capture_all_daily(capture_date, limit=None):
    logger.info("Capture all daily data started.")
    date_string = capture_date.strftime("%d-%m-%Y")
    file_path = f'/opt/airflow/data/outputs/output_daily_{date_string}.xlsx'

    if os.path.exists(file_path):
        os.remove(file_path)

    all_stock_data = capture_all_stock_data(capture_date, file_path, limit)
    # all_fund_data = capture_all_fund_data(capture_date, file_path, limit)
    
    logger.info("Capture all daily data ended.")
    return all_stock_data

#------------------------------------------------------------------------

# Quarterly

# Thông tin tổng quan (Company overview)
def capture_company_overview(*args):
    symbol = args[0]

    try:
        df = company_overview(symbol)
    except Exception as e:
        error_message = f"Error capturing company overview for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Hồ sơ công ty (Company profile)
def capture_company_profile(*args):
    symbol = args[0]

    try:
        df = company_profile(symbol)
    except Exception as e:
        error_message = f"Error capturing company profile for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()
    
    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Danh sách cổ đông (Company large shareholders)
def capture_company_shareholders(*args):
    symbol = args[0]

    try:
        df = company_large_shareholders(symbol)
    except Exception as e:
        error_message = f"Error capturing company large shareholders for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Các chỉ số tài chính cơ bản (Company fundamental ratio)
def capture_fundamental_ratio(*args, mode='', missing_pct=0.8):
    symbol = args[0]

    try:
        df = company_fundamental_ratio(
            symbol=symbol, 
            mode='', 
            missing_pct=missing_pct
        )
    except Exception as e:
        error_message = f"Error capturing company fundamental ratio for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Danh sách công ty con, công ty liên kết (Company subsidiaries listing)
def capture_subsidiaries_listing(*args, page_size=100):
    symbol = args[0]

    try:
        df = company_subsidiaries_listing(
            symbol=symbol, 
            page_size=page_size
        )

    except Exception as e:
        error_message = f"Error capturing company subsidiaries listing for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Ban lãnh đạo công ty (Company officers)
def capture_company_officers(*args, page_size=20):
    symbol = args[0]

    try:
        df = company_officers(
            symbol=symbol, 
            page_size=page_size, 
            page=0
        )

    except Exception as e:
        error_message = f"Error capturing company officers (BOD) for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Chỉ số tài chính cơ bản (Financial ratio)
def capture_financial_ratio(*args, page_size=20):
    symbol = args[0]

    try:
        df = financial_ratio(
            symbol=symbol, 
            report_range='quarterly', 
            is_all=False
        )

    except Exception as e:
        error_message = f"Error capturing financial ratio for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.apply(pd.to_numeric, errors='ignore')    
    return df.iloc[:, 0].T.to_frame().T

# Báo cáo tài chính (Financial report)
'''
- Báo cáo kinh doanh (Income statement)
- Bảng cân đối kế toán (Balance sheet)
- Báo cáo lưu chuyển tiền tệ (Cash flow)
'''
REPORT_TYPE = ['IncomeStatement', 'BalanceSheet', 'CashFlow']
def capture_income_statement(*args):
    symbol = args[0]
    report_type = args[1]

    try:
        if report_type not in REPORT_TYPE:
            raise ValueError(f"Invalid report type '{type}'. Type must be one of {REPORT_TYPE}")
        
        df = financial_report(
            symbol=symbol, 
            report_type=report_type, 
            frequency='Quarterly'
        )

    except Exception as e:
        error_message = f"Error capturing income statement of type {report_type} for symbol {symbol}: {e}"
        logger.error(error_message)
        return pd.DataFrame()

    df = df.iloc[:, [0, -1]].T
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df = df.apply(pd.to_numeric, errors='ignore')
    return df

# Crawl all quarterly data and store
def capture_all_quarterly_data(capture_date, limit=None):
    logger.info("Capture all quarterly data started.")

    date_string = capture_date.strftime("%d-%m-%Y")
    file_path = f'/opt/airflow/data/outputs/output_quarterly_{date_string}.xlsx'

    if os.path.exists(file_path):
        os.remove(file_path)

    company_listing_df = capture_company_listing()
    if limit is not None:
        company_symbols = company_listing_df["ticker"][:limit]
    else:
        company_symbols = company_listing_df["ticker"]

    functions_to_capture = [
        capture_company_overview,
        capture_company_profile,
        capture_company_shareholders,
        capture_fundamental_ratio,
        capture_subsidiaries_listing,
        capture_company_officers,
        capture_financial_ratio
    ]

    data_dict = {f.__name__: process_symbols(company_symbols, f, capture_date) for f in functions_to_capture}

    data_dict.update({r: process_symbols(company_symbols, capture_income_statement, r) for r in REPORT_TYPE})

    export_data_to_excel(data_dict, company_listing_df, 'Company', file_path, "Quarterly")
    
    logger.info("Capture all quarterly data ended.")
    return company_listing_df, *data_dict.values()

#------------------------------------------------------------------------

# Set up logger
def create_logger():
    logger = logging.getLogger('server_logger')
    logger.setLevel(logging.ERROR)
    logger.setLevel(logging.INFO)

    for handler in logger.handlers:
        print(handler)
        logger.removeHandler(handler)

    if not len(logger.handlers) > 0:
        file_handler = logging.FileHandler(f'/opt/airflow/data/logs.log')

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# Capture all data
CAPTURE_OPTION = {
    0: "DAILY",
    1: "QUARTERLY",
    2: "BOTH"
}

logger = create_logger()
def capture_all_data(capture_date, limit=None, option=2):    
    if option not in CAPTURE_OPTION:
        error_message = f"Invalid option '{option}'. Please provide a valid option from {CAPTURE_OPTION}."
        logger.error(error_message)
        raise ValueError(error_message)
    
    if option == 0:
        capture_all_daily(capture_date, limit)
    elif option == 1:
        capture_all_quarterly_data(capture_date, limit)
    elif option == 2:
        capture_all_daily(capture_date, limit)
        capture_all_quarterly_data(capture_date, limit)