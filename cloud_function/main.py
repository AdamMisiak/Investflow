import os
from os.path import basename
from dotenv import load_dotenv
load_dotenv()

from utils.logger import logger
from parsers.multi_section_parser import parse_multi_section_csv, extract_ending_cash_data
from parsers.trade_parser import parse_trades_df
from services.sheets_service import write_to_google_sheets
from services.slack_service import send_slack_message

# Environment variables
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
CSV_FILE = os.getenv("CSV_FILE")  # For local testing
BUCKET_NAME = os.getenv("BUCKET_NAME")

def process_csv(file_path):
    sections = parse_multi_section_csv(file_path)

    # Extract Ending Cash data
    ending_cash = extract_ending_cash_data(sections)
    logger.info(f"Ending Cash data: {ending_cash}")

    stock_transactions = []
    option_transactions = []

    trade_sections = [k for k in sections if k.split("_")[0] == "Trades"]
    if not trade_sections:
        logger.warning("⚠️ No 'Trades' section found.")
        return

    trade_sections.sort()
    first_section = True

    counters = {
        "stocks_processed": 0,
        "stocks_inserted": 0,
        "options_processed": 0,
        "options_inserted": 0
    }

    for tsec in trade_sections:
        df_sec = sections[tsec]
        if first_section:
            st, _ = parse_trades_df(df_sec, is_option=False, counters=counters)
            stock_transactions.extend(st)
            first_section = False
        else:
            _, opt = parse_trades_df(df_sec, is_option=True, counters=counters)
            option_transactions.extend(opt)

    all_tx = stock_transactions + option_transactions
    if all_tx:
        write_to_google_sheets(all_tx)

    # Slack message
    if SLACK_WEBHOOK_URL:
        # Add Ending Cash information to the Slack message
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

def process_gcs_file(event, context):
    """Entry point for Cloud Function"""
    from google.cloud import storage
    
    if not BUCKET_NAME:
        logger.error("BUCKET_NAME environment variable not set")
        return
    
    # Get the file that triggered this function
    file_name = event['name']
    logger.info(f"Processing file: {file_name}")
    
    # Download the file to a temporary location
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    
    # Create a temporary file
    temp_file = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file)
    
    try:
        process_csv(temp_file)
    finally:
        # Clean up
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    logger.info("✅ Cloud function execution completed successfully")

def main():
    """Entry point for local testing"""
    if not CSV_FILE:
        logger.error("No CSV_FILE environment variable specified for local testing.")
        return
    process_csv(CSV_FILE)
    logger.info("✅ Local execution completed successfully")

if __name__ == "__main__":
    main()
