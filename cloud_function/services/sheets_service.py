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

    header = list(data[0].keys())
    rows_to_insert = []
    for record in data:
        if record["transaction_id"] not in existing_ids:
            row = [record.get(col, "") for col in header]
            rows_to_insert.append(row)

    if rows_to_insert:
        # If the header row is missing, add it
        if worksheet.row_count < 1 or not worksheet.row_values(1):
            worksheet.append_row(header)
        worksheet.append_rows(rows_to_insert)
        logger.info(f"✅ Inserted {len(rows_to_insert)} records into Google Sheet: {sheet_name}.")
