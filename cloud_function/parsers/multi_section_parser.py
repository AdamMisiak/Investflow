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


def _finalize_subsection(parsed_sections, section, subsection_counter, header, rows):
    """
    Determines asset type if applicable and saves the current subsection.
    """
    if not header or not rows:
        return

    asset_type_for_subsection = None
    if section.startswith("Trades") and rows:
        first_data_row_fields = rows[0]
        if len(first_data_row_fields) > 1:  # Asset Category is the 2nd field in tail
            asset_type_for_subsection = first_data_row_fields[1].strip()
        else:
            logger.warning(f"Could not determine asset type for Trades section '{section}' (block {subsection_counter[section]}) due to insufficient columns in first data row.")
    
    _save_subsection(parsed_sections, section, subsection_counter, header, rows, asset_type_for_subsection)
    subsection_counter[section] += 1

def _process_temp_sections_to_dataframes(sections_temp):
    """
    Process the temporary section data into DataFrames.
    
    Args:
        sections_temp (defaultdict): Dictionary with section data from _read_csv_to_temp_sections
        
    Returns:
        dict: Dictionary with section names as keys and DataFrames as values
    """
    parsed_sections = {}
    subsection_counter = defaultdict(int) # Counter for blocks within the same original section name
    
    for section, items in sections_temp.items():
        current_header = None
        current_rows = []
        
        for (row_type, row_data) in items:
            if row_type == "Header":
                # Finalize previous subsection if it exists
                _finalize_subsection(parsed_sections, section, subsection_counter, current_header, current_rows)
                
                # Start new subsection
                current_header = row_data
                current_rows = []
            elif row_type == "Data" and current_header is not None:
                current_rows.append(row_data)
        
        # Finalize the last subsection for the current section
        _finalize_subsection(parsed_sections, section, subsection_counter, current_header, current_rows)
            
    return parsed_sections


def _save_subsection(parsed_sections, section, subsection_counter, header, rows, asset_type=None):
    """
    Create a DataFrame from header and rows and save it to parsed_sections.
    
    Args:
        parsed_sections (dict): Dictionary to save the resulting DataFrame
        section (str): The section name (e.g., "Trades")
        subsection_counter (defaultdict): Counter for subsections of the original section name
        header (list): Column names
        rows (list): Data rows
        asset_type (str, optional): The determined asset type for this subsection. Defaults to None.
    """
    # subsection_counter[section] gives the current block number for the original 'section'
    subsec_key = _make_subsection_key(section, subsection_counter[section], asset_type)
    df = _build_df_from_header_and_rows(subsec_key, header, rows)
    if df is not None:
        parsed_sections[subsec_key] = df


def _make_subsection_key(section, counter, asset_type=None):
    """
    Create a key for a subsection based on section name, counter, and asset type.
    
    Args:
        section (str): Section name (e.g., "Trades")
        counter (int): Subsection counter for the original section name
        asset_type (str, optional): Type of the asset (e.g., 'Stocks', 'Equity and Index Options'). Defaults to None.
        
    Returns:
        str: Subsection key
    """
    base_name_to_use = section
    if section.startswith("Trades") and asset_type and asset_type.strip():
        # Form a more specific base name using the asset type
        base_name_to_use = f"{section} {asset_type.strip()}"
        return base_name_to_use
    
    # Append counter if it's not the first block under this name configuration
    return f"{base_name_to_use}" if counter == 0 else f"{base_name_to_use} {counter}"


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


def validate_required_sections(sections):
    required_section_types = ["Trades", "Cash Report"]
    missing_sections = []
    
    # Check for required section types
    for section_type in required_section_types:
        # Look for any sections starting with this type
        matching_sections = [s for s in sections if s.startswith(section_type)]
        if not matching_sections:
            missing_sections.append(section_type)
            logger.warning(f"⚠️ Required section '{section_type}' not found in parsed data")
    
    is_valid = len(missing_sections) == 0
    
    if not is_valid:
        logger.warning(f"❌ Missing required sections: {', '.join(missing_sections)}")
    
    return is_valid, missing_sections
