import pandas as pd
from utils.logger import logger
from utils.helpers import generate_transaction_id
from builders.asset_builder import build_asset_record
from builders.option_builder import build_option_record
from services.supabase_service import insert_batch_to_supabase

def parse_trades_df(df: pd.DataFrame, trade_type: str, counters: dict):
    """
    Parse trades DataFrame into stock, option, or bond records based on trade_type.
    """
    transactions = []
    batch_size = 100  # Process in batches of 100 records

    # Determine target table and record builder based on trade_type
    if trade_type == "options":
        record_builder = build_option_record
        target_table = "option_transactions"
    elif trade_type in ["stocks", "bonds"]:
        record_builder = build_asset_record
        target_table = "asset_transactions"
    else:
        logger.warning(f"Unsupported trade_type: {trade_type}. Skipping.")
        return [], [] # Return empty lists for stock/option to match original structure if needed

    logger.info(f"🔎 Processing {len(df)} {trade_type} row(s)...")
    for idx, row in df.iterrows():

        counters[f"{trade_type}_processed"] += 1
                
        raw_data = clean_nan(row.to_dict())
        
        # Asset category check can be simplified or removed if main.py ensures correct df
        asset_category_map = {
            "stocks": "Stocks",
            "options": "Equity and Index Options",
            "bonds": "Treasury Bills" # Assuming this is the category name in CSV for bonds
        }
        expected_asset_category = asset_category_map.get(trade_type)
        current_asset_category = str(raw_data.get("Asset Category", "")).strip()

        if expected_asset_category and current_asset_category != expected_asset_category:
            logger.info(f"⏩ Skipping row with Asset Category: {current_asset_category} for expected {expected_asset_category}")
            continue

        symbol = str(raw_data.get("Symbol", "")).strip()
        date_time_str = str(raw_data.get("Date/Time", "")).replace(",", "").strip()
        code_str = str(raw_data.get("Code", "")).strip()
        currency = str(raw_data.get("Currency", "USD")).strip()

        quantity_str = str(raw_data.get("Quantity", "")).strip()
        trade_price_str = str(raw_data.get("T. Price", "")).strip()
        fees_str = str(raw_data.get("Comm/Fee", 0)).strip()

        raw_qty = try_float(quantity_str)
        trade_price = try_float(trade_price_str)
        fees = try_float(fees_str)

        side = "sell" if raw_qty < 0 else "buy"
        quantity = raw_qty # Keep original sign for quantity

        code_upper = code_str.upper()
        if code_upper == "O":
            tx_type = "open"
        elif "EP" in code_upper:
            tx_type = "expired"
        else:
            tx_type = "close"

        tx_id = generate_transaction_id(date_time_str, symbol, quantity, trade_price, code_str)

        # NOTE: BONDS STILL FAILING
        record_params = {
            "tx_id": tx_id,
            "executed_at": date_time_str,
            "symbol": symbol,
            "quantity": quantity,
            "trade_price": trade_price,
            "fees": fees, # Use correct fee parameter name
            "code_str": code_str,
            "tx_type": tx_type,
            "side": side,
            "currency": currency,
            "raw_data": raw_data,
        }
        # build_asset_record does not take 'fees', it takes 'fee'
        rec = record_builder(**record_params)
        transactions.append(rec)

    if transactions:
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i + batch_size]
            tx_ids = [r["transaction_id"] for r in batch]
            inserted = insert_batch_to_supabase(target_table, batch, tx_ids)
            counters[f"{trade_type}_inserted"] += inserted
    
    # To maintain compatibility with how results are expected in main.py (stx, otx)
    # This part needs careful handling based on how main.py will use the returned values.
    # For now, let's assume parse_trades_df is called per type, so it returns one list.
    return transactions # Caller will assign to appropriate list (stocks, options, bonds)

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
