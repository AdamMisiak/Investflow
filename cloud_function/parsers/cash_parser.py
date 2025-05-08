import csv
from collections import defaultdict
import pandas as pd
from utils.logger import logger


def extract_ending_cash_data(sections):
    """
    Extracts all Ending Cash data from Cash Report sections.
    
    Args:
        sections: Dictionary of parsed sections from parse_multi_section_csv
        
    Returns:
        Dictionary with currency as key and latest value as value.
        Example: {'PLN': 28048.41445, 'USD': -5993.121253923}
    """
    # Find all Cash Report sections
    cash_report_sections = _find_cash_report_sections(sections)
    
    # Extract cash data from all sections
    ending_cash_data = []
    for section_key in cash_report_sections:
        df = sections[section_key]
        ending_cash_data.extend(_extract_cash_from_dataframe(section_key, df))
    
    # Convert to dictionary with currency as key and value
    return _build_currency_dict(ending_cash_data)


def _find_cash_report_sections(sections):
    """Find all sections related to Cash Reports."""
    return [k for k in sections if k.startswith("Cash Report")]


def _build_currency_dict(cash_data_list):
    """Convert a list of cash data items to a currency-indexed dictionary."""
    result = {}
    for item in cash_data_list:
        result[item["currency"]] = item["value"]
    return result


def _extract_cash_from_dataframe(section_key, df):
    """
    Extract ending cash data from a single DataFrame.
    
    Args:
        section_key (str): Section key for logging
        df (DataFrame): DataFrame to extract data from
        
    Returns:
        list: List of dictionaries with currency and value
    """
    results = []
    
    if df is None or len(df.columns) == 0:
        return results
        
    try:
        # Get the first column name
        first_col = df.columns[0]
        
        # Filter rows containing "Ending Cash"
        ending_cash_rows = df[df[first_col] == "Ending Cash"]
        
        for _, row in ending_cash_rows.iterrows():
            cash_item = _process_cash_row(row)
            if cash_item:
                results.append(cash_item)
                
    except Exception as e:
        logger.warning(f"⚠️ Error extracting Ending Cash data from {section_key}: {e}")
        
    return results


def _process_cash_row(row):
    """Process a single row of cash data."""
    # Need at least 3 columns: description, currency, value
    if len(row) < 3:
        return None
        
    currency_col_idx = 1  # Second column has currency info
    value_col_idx = 2     # Third column has the value
    
    # Use iloc to access by position (fixes FutureWarning)
    if pd.notna(row.iloc[currency_col_idx]) and pd.notna(row.iloc[value_col_idx]):
        currency = row.iloc[currency_col_idx]
        value = row.iloc[value_col_idx]
        
        # Try to convert value to float
        try:
            value = float(value)
        except (ValueError, TypeError):
            pass
            
        return {
            "currency": currency,
            "value": value
        }
    
    return None

def get_csv_file_date(file_name):
    from datetime import datetime
    date_str = file_name.split('_')[-1].split('.')[0]  # Get '20250423'
    return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
