import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.logger import logger

GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def write_to_google_sheets(data, sheet_name="Transactions"):
    """
    Writes a list of dictionaries to a Google Sheets worksheet.
    Only inserts rows not already present (based on transaction_id).
    Only sends specific columns to Google Sheets.
    """
    if not GOOGLE_SHEETS_CREDENTIALS_FILE or not GOOGLE_SHEET_ID:
        logger.warning("Missing Google Sheets configuration.")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, scope)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        worksheet = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")

    existing_records = worksheet.get_all_records()
    existing_ids = {record.get("transaction_id") for record in existing_records if "transaction_id" in record}

    if not data:
        logger.info(f"No new data to insert into Google Sheet: {sheet_name}.")
        return

    # Define the columns we want to include in the sheet
    sheet_columns = [
        "Date", "Category", "Side", "Ticker", "Quantity", "Price", "Fees", "Currency", "Value", "Full Value", "Type"
    ]
    
    # Prepare data for Google Sheets with only the desired columns
    sheets_compatible_data = []
    for record in data:
        # Determine the category based on data structure
        category = "Options" if "option_type" in record and record["option_type"] else "Stocks"
        
        sheets_record = {
            "ID": record["transaction_id"],  # Keep for deduplication
            "Date": record.get("executed_at", ""),
            "Category": category,
            "Side": record.get("side", ""),
            "Ticker": record.get("ticker", ""),
            "Quantity": record.get("quantity", ""),
            "Price": record.get("price", ""),
            "Fees": record.get("fees", ""),
            "Currency": record.get("currency", "USD"),
            "Value": record.get("value", ""),
            "Full Value": record.get("full_value", ""),
            "Type": record.get("type", "")
        }
        sheets_compatible_data.append(sheets_record)
    
    if not sheets_compatible_data:
        logger.info(f"No data to insert after filtering incompatible fields")
        return
        
    header = sheet_columns
    rows_to_insert = []
    for record in sheets_compatible_data:
        if record["ID"] not in existing_ids:
            row = [record.get(col, "") for col in header]
            rows_to_insert.append(row)

    if rows_to_insert:
        # If the header row is missing, add it
        if worksheet.row_count < 1 or not worksheet.row_values(1):
            worksheet.append_row(header)
        worksheet.append_rows(rows_to_insert)
        logger.info(f"✅ Inserted {len(rows_to_insert)} records into Google Sheet: {sheet_name}.")
