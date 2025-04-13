import os
from os.path import basename

from dotenv import load_dotenv
load_dotenv()  # Load environment variables if running locally or in some setups

from utils.logger import logger
from parsers.multi_section_parser import parse_multi_section_csv
from parsers.trade_parser import parse_trades_df
from services.sheets_service import write_to_google_sheets
from services.slack_service import send_slack_message

CSV_FILE = os.getenv("CSV_FILE")  # e.g. "some_report.csv"

def process_csv(file_path):
    sections = parse_multi_section_csv(file_path)

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
    # if all_tx:
    #     write_to_google_sheets(all_tx)

    # Slack message
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
        f"*🔗 Supabase:* <https://supabase.com/dashboard/project/uxpqahwmqpkgqlpzwmof/editor|View in Supabase>"
    )
    send_slack_message(msg)
    logger.info(msg)

def main():
    if not CSV_FILE:
        logger.error("No CSV_FILE environment variable specified.")
        return
    process_csv(CSV_FILE)

if __name__ == "__main__":
    main()
