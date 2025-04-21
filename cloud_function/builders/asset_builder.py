def build_asset_record(
    tx_id,
    executed_at,
    symbol,
    quantity,
    trade_price,
    fee,
    code_str,
    tx_type,
    side,
    currency,
    raw_data
):
    """
    Builds a dictionary for asset (stock) transactions.
    """
    value = quantity * trade_price * -1
    return {
        "transaction_id": tx_id,
        "executed_at": executed_at,
        "ticker": symbol,
        "quantity": quantity,
        "price": trade_price,
        "fees": fee,
        "currency": currency,
        "code": code_str.upper(),
        "type": tx_type,
        "side": side,
        "value": value,
        "full_value": value + fee,
        "raw_data": raw_data,
    }
