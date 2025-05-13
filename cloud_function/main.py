import os
from os.path import basename
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime
from utils.logger import logger
from parsers.multi_section_parser import parse_multi_section_csv, validate_required_sections
from parsers.cash_parser import extract_ending_cash_data, get_csv_file_date
from parsers.trade_parser import parse_trades_df
from services.sheets_service import write_to_google_sheets, write_cash_reports
from services.slack_service import send_slack_message

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
CSV_FILE = os.getenv("CSV_FILE")
BUCKET_NAME = os.getenv("BUCKET_NAME")

def process_cash_report(ending_cash):
    csv_date = get_csv_file_date(CSV_FILE)
    cash_data = []
    
    for currency, value in ending_cash.items():
        if currency == "Base Currency Summary":
            continue
            
        cash_data.append({
            "date": csv_date,
            "currency": currency,
            "value": round(value, 2)
        })
    
    write_cash_reports(cash_data)

def process_csv_file(file_path):
    sections = parse_multi_section_csv(file_path)
    validate_required_sections(sections)
    section_count = len(sections.keys())
    logger.info(f"📊 Found {section_count} parsed sections: {sorted(sections.keys())}")


    # --- CASH ---
    ending_cash = extract_ending_cash_data(sections)
    logger.info(f"💰 Ending Cash data: {ending_cash}")

    if ending_cash:
        process_cash_report(ending_cash)


    # --- TRADES ---
    stock_transactions = []
    option_transactions = []
    bond_transactions = [] # Initialize list for bond transactions

    counters = {
        "stocks_processed": 0,
        "stocks_inserted": 0,
        "options_processed": 0,
        "options_inserted": 0,
        "bonds_processed": 0,
        "bonds_inserted": 0
    }

    for section_name, df_sec in sections.items():
        if not section_name.startswith("Trades"):
            continue

        trade_type = None
        if "Stocks" in section_name:
            trade_type = "stocks"
        elif "Equity and Index Options" in section_name:
            trade_type = "options"
        elif "Treasury Bills" in section_name:
            trade_type = "bonds"
        
        if trade_type:
            logger.info(f"--------------------------------------------------")
            logger.info(f"ℹ️  Processing {trade_type} section: {section_name}")
            transactions = parse_trades_df(df_sec, trade_type=trade_type, counters=counters)
            if trade_type == "stocks":
                stock_transactions.extend(transactions)
            elif trade_type == "options":
                option_transactions.extend(transactions)
            elif trade_type == "bonds":
                bond_transactions.extend(transactions)
        else:
            logger.warning(f"⚠️  Unrecognized Trades section format: {section_name}. Skipping.")

    # --- GOOGLE SHEET ---
    all_tx = stock_transactions + option_transactions + bond_transactions
    if all_tx:
        write_to_google_sheets(all_tx)

    # --- SLACK ---
    if SLACK_WEBHOOK_URL:
        ending_cash_msg = "*💰 Ending Cash:*\n"
        if ending_cash:
            for currency, value in ending_cash.items():
                ending_cash_msg += f"• {currency}: `{round(value, 2)}`\n"
        else:
            ending_cash_msg += "• No Ending Cash data found\n"
        
        msg = (
            f"*📄 CSV File:* `{basename(file_path)}`\n\n"
            f"*🔍 Records Processed:*\n"
            f"• Stocks: `{counters['stocks_processed']}`\n"
            f"• Options: `{counters['options_processed']}`\n"
            f"• Total: `{counters['stocks_processed'] + counters['options_processed']}`\n\n"
            f"*🆕 New Records Added:*\n"
            f"• Stocks: `{counters['stocks_inserted']}`\n"
            f"• Options: `{counters['options_inserted']}`\n"
            f"• Total: `{counters['stocks_inserted'] + counters['options_inserted']}`\n\n"
            f"{ending_cash_msg}\n"
            f"*🔗 Supabase:* <https://supabase.com/dashboard/project/uxpqahwmqpkgqlpzwmof/editor|View in Supabase>\n"
            f"*📋 Google Sheet:* <https://docs.google.com/spreadsheets/d/1Ti9vSwPYyOHrNNsENgHva5m8X70fXut9qPJy5WkxWM4/edit?gid=0#gid=0|View in Google Sheet>\n"
        )
        send_slack_message(msg)
    logger.info(f"Processed file: {basename(file_path)}")

# Google Cloud Function
def main_cloud_function(event, context):
    from google.cloud import storage
    
    if not BUCKET_NAME:
        logger.error("BUCKET_NAME environment variable not set")
        return
    
    file_name = event['name']
    logger.info(f"Processing file: {file_name}")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    
    temp_file = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file)
    
    try:
        process_csv_file(temp_file)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    logger.info("✅ Cloud function execution completed successfully")

# Local
def main_local():
    if not CSV_FILE:
        logger.error("No CSV_FILE environment variable specified for local testing.")
        return
    process_csv_file(CSV_FILE)
    logger.info("✅ Local execution completed successfully")

if __name__ == "__main__":
    main_local()
