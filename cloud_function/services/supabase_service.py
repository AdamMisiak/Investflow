import os
import json
import requests
from utils.logger import logger

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

HEADERS_SUPABASE = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json"
}

def insert_to_supabase(table, data, transaction_id):
    url = f"{SUPABASE_URL}/rest/v1/{table}?transaction_id=eq.{transaction_id}"
    response = requests.get(url, headers=HEADERS_SUPABASE)
    
    if response.status_code == 200 and response.json():
        logger.info(f"ℹ️ Transaction {transaction_id} already exists in {table}. Skipping.")
        return False
    elif response.status_code != 200:
        logger.error(f"🚨 Error checking transaction in Supabase: {response.text}")
        return False

    post_url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.post(post_url, headers=HEADERS_SUPABASE, data=json.dumps(data))
    
    if response.status_code not in [200, 201]:
        logger.error(f"🚨 Error inserting transaction {transaction_id} into {table}: {response.text}")
        return False
    else:
        logger.info(f"✅ Inserted transaction {transaction_id} into {table}.")
        return True
