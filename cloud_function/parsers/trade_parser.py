import pandas as pd
from utils.logger import logger
from utils.helpers import generate_transaction_id
from builders.asset_builder import build_asset_record
from builders.option_builder import build_option_record
from services.supabase_service import insert_to_supabase

def parse_trades_df(df: pd.DataFrame, is_option: bool = False, counters: dict = None):
    """
    Parse trades DataFrame into either stock or option records.
    """
    stx = []
    otx = []

    for idx, row in df.iterrows():
        logger.info(f"🔎 Processing {('Options' if is_option else 'Stock')} row {idx}")

        if counters:  # Increment processed counter
            if is_option:
                counters["options_processed"] += 1
            else:
                counters["stocks_processed"] += 1
                
        raw_data = clean_nan(row.to_dict())

        symbol = str(raw_data.get("Symbol", "")).strip()
        date_time_str = str(raw_data.get("Date/Time", "")).replace(",", "").strip()
        code_str = str(raw_data.get("Code", "")).strip()

        # numeric fields
        quantity_str = str(raw_data.get("Quantity", "")).strip()
        trade_price_str = str(raw_data.get("T. Price", "")).strip()
        fees_str = str(raw_data.get("Comm/Fee", 0)).strip()

        raw_qty = try_float(quantity_str)
        trade_price = try_float(trade_price_str)
        fees = try_float(fees_str)

        side = "sell" if raw_qty < 0 else "buy"
        quantity = raw_qty

        code_upper = code_str.upper()
        tx_type = "open" if code_upper == "O" else "close"

        tx_id = generate_transaction_id(date_time_str, symbol, quantity, trade_price, code_str)

        if is_option:
            rec = build_option_record(
                tx_id=tx_id,
                executed_at=date_time_str,
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
            if insert_to_supabase("option_transactions", rec, tx_id):
                if counters: counters["options_inserted"] += 1
        else:
            rec = build_asset_record(
                tx_id=tx_id,
                executed_at=date_time_str,
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
            if insert_to_supabase("asset_transactions", rec, tx_id):
                if counters: counters["stocks_inserted"] += 1

    return stx, otx

def clean_nan(raw_dict):
    """
    Convert any float('nan') to None in a dict.
    """
    for k, v in raw_dict.items():
        if isinstance(v, float) and pd.isna(v):
            raw_dict[k] = None
    return raw_dict

def try_float(val):
    try:
        return float(val)
    except:
        return 0.0
