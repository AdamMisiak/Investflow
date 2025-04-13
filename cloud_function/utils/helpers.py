import hashlib

def generate_transaction_id(date_time_str, symbol, quantity, trade_price, code):
    """
    Generate a deterministic hash-based transaction ID from fields.
    """
    base_str = f"{date_time_str}_{symbol}_{quantity}_{trade_price}_{code}"
    return hashlib.md5(base_str.encode('utf-8')).hexdigest()
