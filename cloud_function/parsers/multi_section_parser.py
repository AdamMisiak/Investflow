import csv
from collections import defaultdict
import pandas as pd
from utils.logger import logger


def parse_multi_section_csv(file_path):
    """
    Parse a CSV file and extract all sections.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        dict: Dictionary with section names as keys and DataFrames as values
    """
    sections_temp = _read_csv_to_temp_sections(file_path)
    parsed_sections = _process_temp_sections_to_dataframes(sections_temp)
    return parsed_sections


def _read_csv_to_temp_sections(file_path):
    """
    Read the CSV file and organize rows by section in a temporary structure.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        defaultdict: Dictionary with section names as keys and lists of (row_type, row_data) tuples
    """
    sections_temp = defaultdict(list)
    
    try:
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
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
        
    return sections_temp


def _process_temp_sections_to_dataframes(sections_temp):
    """
    Process the temporary section data into DataFrames.
    
    Args:
        sections_temp (defaultdict): Dictionary with section data from _read_csv_to_temp_sections
        
    Returns:
        dict: Dictionary with section names as keys and DataFrames as values
    """
    parsed_sections = {}
    subsection_counter = defaultdict(int)
    
    for section, items in sections_temp.items():
        current_header = None
        current_rows = []
        
        for (row_type, row_data) in items:
            if row_type == "Header":
                # Save previous subsection if exists
                if current_header and current_rows:
                    _save_subsection(parsed_sections, section, subsection_counter, current_header, current_rows)
                    subsection_counter[section] += 1
                
                # Start new subsection
                current_header = row_data
                current_rows = []
            elif row_type == "Data" and current_header is not None:
                current_rows.append(row_data)
        
        # Save the last subsection
        if current_header and current_rows:
            _save_subsection(parsed_sections, section, subsection_counter, current_header, current_rows)
            subsection_counter[section] += 1
            
    return parsed_sections


def _save_subsection(parsed_sections, section, subsection_counter, header, rows):
    """
    Create a DataFrame from header and rows and save it to parsed_sections.
    
    Args:
        parsed_sections (dict): Dictionary to save the resulting DataFrame
        section (str): The section name
        subsection_counter (defaultdict): Counter for subsections
        header (list): Column names
        rows (list): Data rows
    """
    subsec_key = _make_subsection_key(section, subsection_counter[section])
    df = _build_df_from_header_and_rows(subsec_key, header, rows)
    if df is not None:
        parsed_sections[subsec_key] = df


def _make_subsection_key(section, counter):
    """
    Create a key for a subsection based on section name and counter.
    
    Args:
        section (str): Section name
        counter (int): Subsection counter
        
    Returns:
        str: Subsection key
    """
    return f"{section}" if counter == 0 else f"{section} {counter}"


def _build_df_from_header_and_rows(sec_key, header, rows):
    """
    Build a DataFrame from header and rows, handling mismatched column counts.
    
    Args:
        sec_key (str): Section key for logging
        header (list): Column names
        rows (list): Data rows
        
    Returns:
        DataFrame or None: Pandas DataFrame with the data, or None if creation failed
    """
    max_len = len(header)
    cleaned_rows = []
    
    for row in rows:
        if len(row) > max_len:
            row = row[:max_len]
        elif len(row) < max_len:
            row += [None] * (max_len - len(row))
        cleaned_rows.append(row)
    
    try:
        return pd.DataFrame(cleaned_rows, columns=header)
    except Exception as e:
        logger.warning(f"⚠️ Could not parse section {sec_key}: {e}")
        return None
