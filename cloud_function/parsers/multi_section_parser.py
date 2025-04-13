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
