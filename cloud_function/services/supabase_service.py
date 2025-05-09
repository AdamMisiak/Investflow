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
    return insert_batch_to_supabase(table, [data], [transaction_id])

def insert_batch_to_supabase(table, data_list, transaction_ids):
    if not data_list:
        return 0

    # Process in smaller chunks to avoid URL length issues
    chunk_size = 20
    total_inserted = 0
    
    for i in range(0, len(data_list), chunk_size):
        chunk_data = data_list[i:i + chunk_size]
        chunk_ids = transaction_ids[i:i + chunk_size]
        
        # Check existing records using POST with a filter
        check_url = f"{SUPABASE_URL}/rest/v1/{table}"
        check_payload = {
            "select": "transaction_id",
            "transaction_id": f"in.({','.join(chunk_ids)})"
        }
        
        try:
            response = requests.get(
                check_url,
                headers=HEADERS_SUPABASE,
                params=check_payload
            )
            
            if response.status_code != 200:
                logger.error(f"🚨 Error checking transactions in Supabase: {response.text}")
                continue

            existing_ids = {item['transaction_id'] for item in response.json()}
            
            # Filter out existing records
            new_data = []
            for data, tx_id in zip(chunk_data, chunk_ids):
                if tx_id not in existing_ids:
                    new_data.append(data)
            
            if not new_data:
                logger.info(f"ℹ️  [Supabase] Chunk {i//chunk_size + 1} already exists in {table}. Skipping.")
                continue

            # Insert new records in batch
            post_url = f"{SUPABASE_URL}/rest/v1/{table}"
            response = requests.post(post_url, headers=HEADERS_SUPABASE, data=json.dumps(new_data))
            
            if response.status_code not in [200, 201]:
                logger.error(f"🚨 Error inserting batch into {table}: {response.text}")
                continue
            
            inserted_count = len(new_data)
            total_inserted += inserted_count
            logger.info(f"✅ [Supabase] Inserted {inserted_count} records in chunk {i//chunk_size + 1} into {table}.")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"🚨 Network error while processing chunk {i//chunk_size + 1}: {str(e)}")
            continue

    if total_inserted > 0:
        logger.info(f"✅ [Supabase] Total inserted records: {total_inserted}")
    return total_inserted
