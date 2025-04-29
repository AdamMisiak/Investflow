import pandas as pd
from utils.logger import logger
from utils.helpers import generate_transaction_id
from builders.asset_builder import build_asset_record
from builders.option_builder import build_option_record
from services.supabase_service import insert_batch_to_supabase

def parse_trades_df(df: pd.DataFrame, is_option: bool = False, counters: dict = None):
    """
    Parse trades DataFrame into either stock or option records.
    """
    stx = []
    otx = []
    batch_size = 100  # Process in batches of 100 records

    for idx, row in df.iterrows():
        logger.info(f"🔎 Processing {('Options' if is_option else 'Stock')} row {idx}")

        if counters:  # Increment processed counter
            if is_option:
                counters["options_processed"] += 1
            else:
                counters["stocks_processed"] += 1
                
        raw_data = clean_nan(row.to_dict())
        
        # Filter only stock/equity or options records
        asset_category = str(raw_data.get("Asset Category", "")).strip()
        if asset_category not in ["Stocks", "Equity and Index Options"]:
            logger.info(f"⏩ Skipping row with Asset Category: {asset_category}")
            continue

        symbol = str(raw_data.get("Symbol", "")).strip()
        date_time_str = str(raw_data.get("Date/Time", "")).replace(",", "").strip()
        code_str = str(raw_data.get("Code", "")).strip()
        currency = str(raw_data.get("Currency", "USD")).strip()  # Extract currency, default to USD

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
                currency=currency,
                raw_data=raw_data,
            )
            otx.append(rec)
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
                currency=currency,
                raw_data=raw_data,
            )
            stx.append(rec)

    # Process stock transactions in batches
    if stx:
        for i in range(0, len(stx), batch_size):
            batch = stx[i:i + batch_size]
            tx_ids = [rec["transaction_id"] for rec in batch]
            inserted = insert_batch_to_supabase("asset_transactions", batch, tx_ids)
            if counters: counters["stocks_inserted"] += inserted

    # Process option transactions in batches
    if otx:
        for i in range(0, len(otx), batch_size):
            batch = otx[i:i + batch_size]
            tx_ids = [rec["transaction_id"] for rec in batch]
            inserted = insert_batch_to_supabase("option_transactions", batch, tx_ids)
            if counters: counters["options_inserted"] += inserted

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
