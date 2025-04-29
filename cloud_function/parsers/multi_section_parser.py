import csv
from collections import defaultdict
from utils.logger import logger
import pandas as pd

def parse_multi_section_csv(file_path):
    """
    Modified parser that handles multiple Header lines in the same 'Trades' section.
    Each time we see a new Header for the same section, we create a new sub-section, e.g. 'Trades_2'.
    """
    sections_temp = defaultdict(list)

    # Read lines from CSV
    with open(file_path, "r", encoding="utf-8-sig") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split(',', 2)
            if len(parts) < 3:
                continue

            section = parts[0].strip()
            row_type = parts[1].strip()
            tail = parts[2]

            parsed_row = next(csv.reader([tail], quotechar='"', skipinitialspace=True))
            sections_temp[section].append((row_type, parsed_row))

    parsed_sections = {}
    subsection_counter = defaultdict(int)

    for section, items in sections_temp.items():
        current_header = None
        current_rows = []

        for (row_type, row_data) in items:
            if row_type == "Header":
                # If we already had a header and rows, save that subsection
                if current_header is not None and current_rows:
                    subsec_key = make_subsection_key(section, subsection_counter[section])
                    df = build_df_from_header_and_rows(subsec_key, current_header, current_rows)
                    if df is not None:
                        parsed_sections[subsec_key] = df
                    subsection_counter[section] += 1

                # Start new header
                current_header = row_data
                current_rows = []
            elif row_type == "Data":
                if current_header is None:
                    continue
                current_rows.append(row_data)

        # leftover for final
        if current_header is not None and current_rows:
            subsec_key = make_subsection_key(section, subsection_counter[section])
            df = build_df_from_header_and_rows(subsec_key, current_header, current_rows)
            if df is not None:
                parsed_sections[subsec_key] = df

            subsection_counter[section] += 1

    return parsed_sections

def make_subsection_key(section, counter):
    return f"{section}" if counter == 0 else f"{section}_{counter}"

def build_df_from_header_and_rows(sec_key, header, rows):
    max_len = len(header)
    cleaned_rows = []
    for r in rows:
        if len(r) > max_len:
            logger.warning(f"⚠️ {sec_key}: data row has too many columns ({len(r)} > {max_len}), trimming.")
            r = r[:max_len]
        elif len(r) < max_len:
            logger.warning(f"⚠️ {sec_key}: data row has too few columns ({len(r)} < {max_len}), padding with None.")
            r += [None]*(max_len - len(r))
        cleaned_rows.append(r)

    try:
        df = pd.DataFrame(cleaned_rows, columns=header)
        return df
    except Exception as e:
        logger.warning(f"⚠️ Could not parse section {sec_key}: {e}")
        return None

def extract_ending_cash_data(sections):
    """
    Extracts all Ending Cash data from Cash Report sections.
    
    Args:
        sections: Dictionary of parsed sections from parse_multi_section_csv
        
    Returns:
        Dictionary with currency as key and latest value as value.
        Example: {'PLN': 28048.41445, 'USD': -5993.121253923}
    """
    ending_cash_data = []
    
    # Find all Cash Report sections
    cash_report_sections = [k for k in sections if k.split("_")[0] == "Cash Report"]
    
    for section_key in cash_report_sections:
        df = sections[section_key]
        
        # Search for rows where the first column contains "Ending Cash"
        if df is not None and len(df.columns) > 0:
            try:
                # Get the column names
                first_col = df.columns[0]
                
                # Filter rows containing "Ending Cash"
                ending_cash_rows = df[df[first_col] == "Ending Cash"]
                
                for _, row in ending_cash_rows.iterrows():
                    # Look for columns that might contain currency info
                    # The format seems to be: "Ending Cash, PLN, value" or "Ending Cash, USD, value"
                    if len(row) >= 3:  # Need at least 3 columns
                        # Try to determine if we have a currency column
                        currency_col_idx = 1  # Assuming the second column has currency info
                        value_col_idx = 2     # Assuming the third column has the value
                        
                        # Extract currency and value if available
                        if pd.notna(row[currency_col_idx]) and pd.notna(row[value_col_idx]):
                            currency = row[currency_col_idx]
                            value = row[value_col_idx]
                            
                            # Try to convert value to float if it's a numeric string
                            try:
                                value = float(value)
                            except (ValueError, TypeError):
                                # If can't convert to float, keep as is
                                pass
                                
                            ending_cash_data.append({
                                "currency": currency,
                                "value": value
                            })
            except Exception as e:
                logger.warning(f"⚠️ Error extracting Ending Cash data from {section_key}: {e}")
    
    # Convert to dictionary with currency as key and latest value as value
    result = {}
    for item in ending_cash_data:
        result[item["currency"]] = item["value"]
        
    return result
