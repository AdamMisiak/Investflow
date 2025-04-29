import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.logger import logger
import time
from typing import List, Dict, Set, Any, Optional
from datetime import datetime

GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
BATCH_SIZE = 100  # Maximum size for batch operations

def write_to_google_sheets(data: List[Dict[str, Any]], sheet_name: str = "Transactions") -> None:
    if not _validate_config():
        return
        
    # Connect and get worksheet
    worksheet = _get_worksheet(sheet_name)
    if not worksheet:
        return
        
    # Define columns and get existing IDs
    columns = _get_columns()
    existing_ids = _get_existing_ids(worksheet, len(columns))
    
    # Process and insert new records
    new_records = _prepare_new_records(data, existing_ids, columns)
    if new_records:
        _insert_records(worksheet, new_records)
    else:
        logger.info("🔄 No new records to insert.")

def _validate_config() -> bool:
    if not GOOGLE_SHEETS_CREDENTIALS_FILE or not GOOGLE_SHEET_ID:
        logger.warning("⚠️ Missing Google Sheets configuration.")
        return False
    return True

def _get_worksheet(sheet_name: str) -> Optional[gspread.Worksheet]:
    try:
        logger.info("🔌 Connecting to Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, scope)
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
        
        try:
            worksheet = sh.worksheet(sheet_name)
            logger.info(f"📄 Accessed worksheet: {sheet_name}")
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=sheet_name, rows="2000", cols="35")  # Increased column count
            logger.info(f"📝 Created new worksheet: {sheet_name}")
            
        return worksheet
    except Exception as e:
        logger.error(f"❌ Error connecting to Google Sheets: {e}")
        return None

def _get_columns() -> List[str]:
    return [
        "Date", "Category", "Side", "Ticker", "Name", "Quantity", 
        "Price", "Fees", "Currency", "Value", "Full Value", "Type",
        "Option Type", "Option Strategy", "Option Expiration Date", "Option Strike Price", 
        "Option Premium", "Option Full Name"
    ]

def _get_existing_ids(worksheet: gspread.Worksheet, visible_cols_count: int) -> Set[str]:
    logger.info("🔍 Reading existing transaction IDs...")
    existing_ids = set()
    hidden_id_col = visible_cols_count + 1  # ID column after visible columns
    
    try:
        # Get all IDs from the hidden column
        if worksheet.row_count > 1:
            id_column = worksheet.col_values(hidden_id_col)
            # Skip header row if present
            existing_ids = set(id for id in id_column[1:] if id)
            logger.info(f"📊 Found {len(existing_ids)} existing transaction IDs")
    except Exception as e:
        logger.error(f"❌ Error reading transaction IDs: {e}")
    
    # Ensure worksheet has headers if it's empty
    if worksheet.row_count < 1:
        try:
            header_row = _get_columns() + ["_transaction_id"]
            worksheet.append_row(header_row)
            logger.info(f"🏷️ Added header row with {len(header_row)} columns to empty worksheet")
            time.sleep(1)  # Allow time for update
        except Exception as e:
            logger.error(f"❌ Error adding header row: {e}")
    
    return existing_ids

def _format_option_expiration(expiration_date: str) -> str:
    if not expiration_date:
        return ""
    
    # Log the original format for debugging
    logger.info(f"🔍 Parsing expiration date: {expiration_date}")
    
    try:
        # Handle special format like "04APR25"
        if expiration_date and len(expiration_date) >= 7 and expiration_date[2:5].isalpha():
            # Extract components
            day = expiration_date[:2]
            month_str = expiration_date[2:5].upper()
            year_str = expiration_date[5:7]
            
            # Month mapping
            month_map = {
                "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
            }
            
            # Convert to datetime object
            if month_str in month_map:
                month = month_map[month_str]
                # Determine century (20xx or 19xx)
                year = int(year_str)
                if year < 50:  # Assume 21st century for years < 50
                    year += 2000
                else:
                    year += 1900
                
                # Create datetime and format
                dt = datetime(year, month, int(day))
                formatted_date = dt.strftime("%Y-%m-%d")
                return formatted_date
        
                
        # If we get here, no format worked
        logger.warning(f"⚠️ Could not parse date format: {expiration_date}")
    except Exception as e:
        logger.error(f"❌ Error parsing expiration date '{expiration_date}': {str(e)}")
    
    # If parsing fails, return original
    return expiration_date

def _format_option_full_name(record: Dict[str, Any]) -> str:
    ticker = record.get("ticker", "")
    if not ticker or "option_type" not in record or not record["option_type"]:
        return ""
    
    try:
        # Get option details
        expiration = _format_option_expiration(record.get("expiration_date", ""))
        strike = record.get("strike_price", "")
        option_type = record.get("option_type", "").upper()
        side = record.get("side", "").upper()
        
        year_suffix = "'" if expiration and len(expiration) > 5 else ""
        strike_str = f"{strike}$" if strike else ""
        
        return f"{ticker} {expiration}{year_suffix} {strike_str} {side} {option_type}".strip()
    except Exception as e:
        logger.warning(f"⚠️ Error formatting option name: {e}")
        return ""

def _prepare_new_records(
    data: List[Dict[str, Any]], 
    existing_ids: Set[str],
    columns: List[str]
) -> List[List[Any]]:
    logger.info(f"🔄 Processing {len(data)} transactions...")
    new_records = []
    
    for record in data:
        tx_id = record["transaction_id"]
        if tx_id not in existing_ids:
            # Determine if this is an option record
            is_option = "option_type" in record and record["option_type"]
            category = "Options" if is_option else "Stocks"
            
            # Build row with values in the correct order
            row = []
            row.append(tx_id)
            for col in columns:
                if col == "Date":
                    row.append(record.get("executed_at", "").split()[0])
                elif col == "Category":
                    row.append(category)
                elif col == "Name":
                    row.append("")  # Empty Name column as requested
                elif col == "Currency":
                    row.append(record.get("currency", "USD"))
                elif col == "Full Value":
                    row.append(record.get("full_value", ""))
                # Option-specific columns
                elif col == "Option Type" and is_option:
                    row.append(record.get("option_type", "").upper())  # PUT/CALL
                elif col == "Option Strategy":
                    row.append("")  # Empty for now as requested
                elif col == "Option Expiration Date" and is_option:
                    row.append(_format_option_expiration(record.get("expiration_date", "")))
                elif col == "Option Strike Price" and is_option:
                    row.append(record.get("strike_price", ""))
                elif col == "Option Premium" and is_option:
                    # For premium, positive means received (credit), negative means paid (debit)
                    premium = float(record.get("value", 0) or 0)
                    row.append(premium)
                elif col == "Option Full Name" and is_option:
                    row.append(_format_option_full_name(record))
                elif col.startswith("Option") and not is_option:
                    row.append("")  # Empty for non-option records
                else:
                    # For other columns, use lowercase column name as the key
                    key = col.lower()
                    row.append(record.get(key, ""))
            
            new_records.append(row)
    
    logger.info(f"🆕 Found {len(new_records)} new transactions to add")
    return new_records

def _resize_if_needed(worksheet: gspread.Worksheet, needed_rows: int) -> None:
    """Resize worksheet if it doesn't have enough rows."""
    if worksheet.row_count < needed_rows + 2:  # +2 for header and buffer
        try:
            new_size = worksheet.row_count + needed_rows + 100  # Add buffer
            logger.info(f"📏 Resizing worksheet from {worksheet.row_count} to {new_size} rows")
            worksheet.resize(rows=new_size)
            time.sleep(1)  # Allow time for resize
        except Exception as e:
            logger.error(f"❌ Error resizing worksheet: {e}")

def _insert_records(worksheet: gspread.Worksheet, records: List[List[Any]]) -> None:
    if not records:
        return
        
    # Check if sheet needs resizing
    _resize_if_needed(worksheet, len(records))
    
    # Insert records in batches
    total_batches = (len(records) - 1) // BATCH_SIZE + 1
    successful_inserts = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        chunk = records[i:i+BATCH_SIZE]
        
        try:
            logger.info(f"📥 Inserting batch {batch_num}/{total_batches} ({len(chunk)} records)")
            worksheet.append_rows(chunk)
            successful_inserts += len(chunk)
            
            # Progress indicator
            progress = min(100, int(batch_num * 100 / total_batches))
            logger.info(f"⏳ Progress: {progress}% ({successful_inserts}/{len(records)} records)")
            
            time.sleep(1)  # Avoid rate limiting
        except Exception as e:
            logger.error(f"❌ Error inserting batch {batch_num}: {e}")
    
    if successful_inserts == len(records):
        logger.info(f"✅ Successfully inserted all {successful_inserts} new transactions")
    else:
        logger.info(f"⚠️ Inserted {successful_inserts} out of {len(records)} transactions")
