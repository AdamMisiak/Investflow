import os
import hashlib
import pandas as pd
import logging
import requests
import json
import csv
from collections import defaultdict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")  # np. "service_account.json"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")            # ID arkusza
CSV_FILE = os.getenv("CSV_FILE")



HEADERS_SUPABASE = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json"
}

def generate_transaction_id(date_time_str, symbol, quantity, trade_price, code):
    base_str = f"{date_time_str}_{symbol}_{quantity}_{trade_price}_{code}"
    return hashlib.md5(base_str.encode('utf-8')).hexdigest()

def insert_to_supabase(table, data, transaction_id):
    url = f"{SUPABASE_URL}/rest/v1/{table}?transaction_id=eq.{transaction_id}"
    response = requests.get(url, headers=HEADERS_SUPABASE)
    if response.status_code == 200 and response.json():
        logging.info(f"ℹ️ Transaction {transaction_id} already exists in {table}. Skipping.")
        return
    elif response.status_code != 200:
        logging.error(f"🚨 Error checking transaction in Supabase: {response.text}")
        return

    post_url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.post(post_url, headers=HEADERS_SUPABASE, data=json.dumps(data))
    if response.status_code not in [200, 201]:
        logging.error(f"🚨 Error inserting transaction {transaction_id} into {table}: {response.text}")
    else:
        logging.info(f"✅ Inserted transaction {transaction_id} into {table}.")

def send_slack_message(message):
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL not configured.")
        return
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if response.status_code != 200:
        logging.error(f"Slack message failed: {response.text}")

def write_to_google_sheets(data, sheet_name="Transactions"):
    if not GOOGLE_SHEETS_CREDENTIALS_FILE or not GOOGLE_SHEET_ID:
        logging.warning("Missing Google Sheets configuration.")
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

    rows_to_insert = []
    if data:
        header = list(data[0].keys())
    else:
        header = []

    for record in data:
        if record["transaction_id"] not in existing_ids:
            row = [record.get(col, "") for col in header]
            rows_to_insert.append(row)

    if rows_to_insert:
        if worksheet.row_count < 1 or not worksheet.row_values(1):
            worksheet.append_row(header)
        worksheet.append_rows(rows_to_insert)
        logging.info(f"✅ Inserted {len(rows_to_insert)} records into Google Sheet: {sheet_name}.")

def parse_multi_section_csv(file_path):
    """
    Modified parser that handles multiple Header lines in the same 'Trades' section.
    Each time we see a new Header for the same section, we create a new sub-section, e.g. 'Trades_2'.
    """
    sections_temp = defaultdict(list)

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
                if current_header is not None and current_rows:
                    subsec_key = section
                    if subsection_counter[section] > 0:
                        subsec_key = f"{section}_{subsection_counter[section]}"
                    df = build_df_from_header_and_rows(subsec_key, current_header, current_rows)
                    if df is not None:
                        parsed_sections[subsec_key] = df

                    subsection_counter[section] += 1

                current_header = row_data
                current_rows = []
            elif row_type == "Data":
                if current_header is None:
                    continue
                current_rows.append(row_data)

        # leftover
        if current_header is not None and current_rows:
            subsec_key = section
            if subsection_counter[section] > 0:
                subsec_key = f"{section}_{subsection_counter[section]}"
            df = build_df_from_header_and_rows(subsec_key, current_header, current_rows)
            if df is not None:
                parsed_sections[subsec_key] = df

            subsection_counter[section] += 1

    return parsed_sections

def build_df_from_header_and_rows(sec_key, header, rows):
    max_len = len(header)
    cleaned_rows = []
    for r in rows:
        if len(r) > max_len:
            logging.warning(f"⚠️ {sec_key}: data row has too many columns ({len(r)} > {max_len}), trimming.")
            r = r[:max_len]
        elif len(r) < max_len:
            logging.warning(f"⚠️ {sec_key}: data row has too few columns ({len(r)} < {max_len}), padding with None.")
            r += [None]*(max_len - len(r))
        cleaned_rows.append(r)

    try:
        df = pd.DataFrame(cleaned_rows, columns=header)
        return df
    except Exception as e:
        logging.warning(f"⚠️ Could not parse section {sec_key}: {e}")
        return None

########################################
# Asset logic (unchanged)
########################################

def build_asset_record(
    tx_id: str,
    executed_at: str,
    symbol: str,
    quantity: float,
    trade_price: float,
    fee: float,
    code_str: str,
    tx_type: str,
    side: str,
    raw_data: dict,
) -> dict:
    """Builds a dictionary for asset (stock) transactions."""
    value = quantity * trade_price * -1
    return {
        "transaction_id": tx_id,
        "executed_at": executed_at,
        "ticker": symbol,
        "quantity": quantity,
        "price": trade_price,
        "fees": fee,
        "code": code_str.upper(),
        "type": tx_type,
        "side": side,
        "value": value,
        "full_value": value + fee,
        "raw_data": raw_data,
    }

########################################
# Option logic (with side from quantity)
########################################

def parse_option_symbol(symbol: str):
    """
    Parse typical IB option symbol format: 'ANET 21FEB25 107 P'
      -> underlying='ANET', expiration_date='21FEB25', strike_price=107.0, option_type='PUT'
    """
    parts = symbol.split()
    underlying = None
    expiration_date = None
    strike_price = None
    option_type = None

    if len(parts) >= 4:
        underlying = parts[0]  # e.g. 'ANET'
        expiration_date = parts[1]  # e.g. '21FEB25'
        try:
            strike_price = float(parts[2])
        except:
            strike_price = None
        # 'P' or 'C'
        if parts[3].upper() == "P":
            option_type = "PUT"
        elif parts[3].upper() == "C":
            option_type = "CALL"
        else:
            option_type = parts[3].upper()
    else:
        # fallback if format is unexpected
        underlying = symbol

    return underlying, strike_price, expiration_date, option_type

def build_option_record(
    tx_id: str,
    executed_at: str,
    symbol: str,
    quantity: float,
    trade_price: float,
    fees: float,
    code_str: str,
    tx_type: str,
    side: str,
    raw_data: dict,
) -> dict:
    """
    Builds a dictionary specifically for option transactions.
    We do NOT store 'currency' here, as requested.
    We do NOT store 'asset_category' here, as requested.
    We interpret side from sign of quantity externally; also open/close from code.
    """
    underlying, strike_price, expiration_date, opt_type = parse_option_symbol(symbol)
    value = quantity * trade_price * 100.0 * -1

    return {
        "transaction_id": tx_id,
        "executed_at": executed_at,
        "ticker": underlying,
        "option_type": opt_type,  # CALL / PUT
        "strike_price": strike_price,
        "expiration_date": expiration_date,
        "quantity": quantity,
        "price": trade_price,
        "fees": fees,
        "code": code_str.upper(),
        "type": tx_type,          # open / close
        "side": side,             # buy / sell
        "value": value,
        "full_value": value + fees, # fee is negative
        "raw_data": raw_data,
    }

########################################
# Parsing the 'Trades' sections
########################################

def parse_trades_df(df: pd.DataFrame, is_option: bool = False):
    """
    If is_option=False => build asset_records
    If is_option=True => build option_records
    """

    stx = []
    otx = []

    for idx, row in df.iterrows():
        logging.info(f"🔎 Processing {('Options' if is_option else 'Stock')} row {idx}")

        raw_data = row.to_dict()

        # Some values might be NaN; convert them to None
        for k, v in raw_data.items():
            if isinstance(v, float) and pd.isna(v):
                raw_data[k] = None

        symbol = str(row.get("Symbol", "")).strip()
        date_time_str = str(row.get("Date/Time", "")).strip()
        date_time_str = date_time_str.replace(",", "")

        quantity_str = str(row.get("Quantity", "")).strip()
        trade_price_str = str(row.get("T. Price", "")).strip()
        fees = float(row.get("Comm/Fee", "").strip())
        code_str = str(row.get("Code", "")).strip()

        executed_at = date_time_str if date_time_str else None

        # parse numeric fields
        try:
            raw_qty = float(quantity_str) if quantity_str else 0.0
        except:
            raw_qty = 0.0
            logging.warning(f"⚠️ Cannot parse Quantity: '{quantity_str}' row={idx}")

        try:
            trade_price = float(trade_price_str) if trade_price_str else 0.0
        except:
            trade_price = 0.0
            logging.warning(f"⚠️ Cannot parse T. Price: '{trade_price_str}' row={idx}")

        # interpret side from sign of quantity
        side = "sell" if raw_qty < 0 else "buy"
        quantity = raw_qty

        # interpret open/close from code
        code_upper = code_str.upper()
        if code_upper == "O":
            tx_type = "open"
        else:
            tx_type = "close"

        tx_id = generate_transaction_id(executed_at, symbol, quantity, trade_price, code_str)

        if is_option:
            rec = build_option_record(
                tx_id=tx_id,
                executed_at=executed_at,
                symbol=symbol,
                quantity=quantity,
                trade_price=trade_price,
                fees=fees,
                code_str=code_str,
                tx_type=tx_type,
                side=side,
                raw_data=raw_data,
            )
            otx.append(rec)
            insert_to_supabase("option_transactions", rec, tx_id)
        else:
            # treat as asset
            rec = build_asset_record(
                tx_id=tx_id,
                executed_at=executed_at,
                symbol=symbol,
                quantity=quantity,
                trade_price=trade_price,
                fee=fees,
                code_str=code_str,
                tx_type=tx_type,
                side=side,
                raw_data=raw_data,
            )
            stx.append(rec)
            insert_to_supabase("asset_transactions", rec, tx_id)

    return stx, otx

def process_csv(file_path):
    sections = parse_multi_section_csv(file_path)

    stock_transactions = []
    option_transactions = []

    trade_sections = [k for k in sections.keys() if k.startswith("Trades")]  # e.g. ["Trades", "Trades_1"]
    if not trade_sections:
        logging.warning("⚠️ No 'Trades' section found.")
        return

    # sort them so we do first => stock, subsequent => options
    trade_sections.sort()
    trade_sections = trade_sections[:2]

    first_section = True
    for tsec in trade_sections:
        df_sec = sections[tsec]
        if first_section:
            # parse as stocks
            st, _ = parse_trades_df(df_sec, is_option=False)
            stock_transactions.extend(st)
            first_section = False
        else:
            # parse as options
            _, opt = parse_trades_df(df_sec, is_option=True)
            option_transactions.extend(opt)

    # all_tx = stock_transactions + option_transactions
    # if all_tx:
    #     write_to_google_sheets(all_tx)

    total_rows = sum(len(sections[tsec]) for tsec in trade_sections)
    msg = (
        f"Processed {total_rows} trades rows across sections. "
        f"Stocks: {len(stock_transactions)}, Options: {len(option_transactions)}."
    )
    send_slack_message(msg)

if __name__ == "__main__":
    process_csv(CSV_FILE)