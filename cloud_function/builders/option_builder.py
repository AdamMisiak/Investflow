def parse_option_symbol(symbol):
    """
    Parses typical IB option symbol format: 'ANET 21FEB25 107 P'
    -> underlying='ANET', expiration_date='21FEB25', strike_price=107.0, option_type='PUT'
    """
    parts = symbol.split()
    if len(parts) < 4:
        return symbol, None, None, None  # fallback in case format is unexpected

    underlying = parts[0]
    expiration_date = parts[1]
    try:
        strike_price = float(parts[2])
    except:
        strike_price = None

    last_part = parts[3].upper()
    option_type = "CALL" if last_part == "C" else "PUT" if last_part == "P" else last_part

    return underlying, strike_price, expiration_date, option_type

def build_option_record(
    tx_id,
    executed_at,
    symbol,
    quantity,
    trade_price,
    fees,
    code_str,
    tx_type,
    side,
    raw_data
):
    """
    Builds a dictionary specifically for option transactions.
    We do NOT store 'currency' or 'asset_category' here.
    We interpret side from sign of quantity externally; also open/close from code.
    """
    underlying, strike_price, expiration_date, opt_type = parse_option_symbol(symbol)
    value = quantity * trade_price * 100.0 * -1

    return {
        "transaction_id": tx_id,
        "executed_at": executed_at,
        "ticker": underlying,
        "option_type": opt_type,
        "strike_price": strike_price,
        "expiration_date": expiration_date,
        "quantity": quantity,
        "price": trade_price,
        "fees": fees,
        "code": code_str.upper(),
        "type": tx_type,  # open/close
        "side": side,     # buy/sell
        "value": value,
        "full_value": value + fees,
        "raw_data": raw_data,
    }
