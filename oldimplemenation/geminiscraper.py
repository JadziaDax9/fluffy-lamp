import asyncio
import aiohttp
import aiosqlite
import os
import logging
import json
from urllib.parse import urlparse, quote # Import quote for encoding credentials
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple, Union, Set # Added Set
import itertools # Keep for potential fallback cycling if needed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# --- Configuration ---
NEXT_PUBLIC_SUPABASE_URL: Optional[str] = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
NEXT_PUBLIC_SUPABASE_ANON_KEY: Optional[str] = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
PROXY_USERNAME: Optional[str] = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD: Optional[str] = os.getenv("PROXY_PASSWORD")
PROXY_PORT: int = 89 # Fixed Port
PROXY_FILE: str = "nordvpnserver.json"

TCGPLAYER_API_URL_TEMPLATE: str = "https://mpapi.tcgplayer.com/v2/product/{}/latestsales"
DB_NAME: str = "fetched_data.db"
INPUT_TABLE_NAME: str = "cards"
INPUT_ID_COLUMN: str = "tcgplayer_id"
DATA_TABLE_NAME: str = "tcgplayer_sales"

# --- Increased Concurrency ---
MAX_CONCURRENT_REQUESTS: int = 1 # <<< Increase Request Amount
# --- Request/Retry Limits ---
REQUEST_LIMIT: int = 25 # Increased API request limit per page (check if API allows this)
MAX_RETRIES_PER_ID: int = 5 # Max attempts per ID (including proxy changes)

REQUEST_HEADERS: Dict[str, str] = { "cookie": "", "accept": "application/json, text/plain, */*", "accept-language": "en-US,en;q=0.9,la;q=0.8", "content-type": "application/json", "dnt": "1", "origin": "https://www.tcgplayer.com", "priority": "u=1, i", "referer": "https://www.tcgplayer.com/", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36" }
REQUEST_QUERYSTRING: Dict[str, str] = {"mpfev": "3302"}
BASE_PAYLOAD: Dict[str, Union[List[Any], str, int]] = { "conditions": [], "languages": [1], "variants": [], "listingType": "All", "limit": REQUEST_LIMIT, "offset": 0 } # Use updated REQUEST_LIMIT

# Type alias for Proxy Dictionary
ProxyDict = Dict[str, Union[str, int, None]]

# --- Custom Exception for Proxy Failures ---
class ProxyError(Exception):
    """Custom exception to signal a likely proxy-related failure."""
    pass

# --- Database and Supabase Functions (largely unchanged) ---

async def setup_database(db_conn: aiosqlite.Connection):
    await db_conn.execute(f"""CREATE TABLE IF NOT EXISTS {DATA_TABLE_NAME} (id TEXT PRIMARY KEY, sales_data TEXT, fetch_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)""")
    await db_conn.commit(); logging.info(f"DB '{DB_NAME}' setup complete.")

async def store_data(db_conn: aiosqlite.Connection, item_id: str, aggregated_sales_data: str):
    try:
        await db_conn.execute(f"INSERT OR REPLACE INTO {DATA_TABLE_NAME} (id, sales_data) VALUES (?, ?)", (item_id, aggregated_sales_data))
        await db_conn.commit(); logging.debug(f"Stored data ID: {item_id}")
    except aiosqlite.Error as e: logging.error(f"SQLite error storing ID {item_id}: {e}")

def get_supabase_client() -> Optional[Client]:
    if not NEXT_PUBLIC_SUPABASE_URL or not NEXT_PUBLIC_SUPABASE_ANON_KEY: logging.error("Supabase env vars missing."); return None
    return create_client(NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY)

async def fetch_ids_from_supabase(supabase: Client) -> List[str]:
    if not supabase: return []
    try:
        loop = asyncio.get_running_loop()
        logging.info(f"Fetching '{INPUT_ID_COLUMN}' from '{INPUT_TABLE_NAME}'...")
        response = await loop.run_in_executor( None, lambda: supabase.table(INPUT_TABLE_NAME).select(INPUT_ID_COLUMN).execute())
        if response.data:
            ids = [str(item[INPUT_ID_COLUMN]) for item in response.data if INPUT_ID_COLUMN in item and item[INPUT_ID_COLUMN] is not None]
            valid_ids = [id_ for id_ in ids if id_.strip()]
            logging.info(f"Fetched {len(valid_ids)} valid IDs from Supabase.")
            return valid_ids
        else:
            logging.warning(f"No data returned from Supabase table '{INPUT_TABLE_NAME}'. Response: {response}")
            return []
    except Exception as e:
        logging.error(f"Error fetching IDs from Supabase: {e}", exc_info=True)
        return []

# --- Load Proxies (unchanged, filters for 'HTTP Proxy (SSL)') ---
def load_proxies_from_file(
    filepath: str,
    port: int,
    username: Optional[str],
    password: Optional[str]
) -> List[ProxyDict]:
    formatted_proxies: List[ProxyDict] = []
    try:
        with open(filepath, 'r') as file: data = json.load(file)
        logging.info(f"Loaded raw server data from {filepath}")
    except Exception as e:
        logging.error(f"Error reading or parsing proxy file {filepath}: {e}")
        return []

    if username is None or password is None:
        logging.warning("Proxy credentials missing. Proxies configured without authentication.")

    count = 0
    for server in data:
        if "hostname" in server and "technologies" in server and isinstance(server["technologies"], list):
            has_specific_proxy = any(
                isinstance(tech, dict) and tech.get("name") == "HTTP Proxy (SSL)"
                for tech in server["technologies"]
            )
            if has_specific_proxy:
                # URL-encode username/password if they contain special characters
                safe_username = quote(username, safe='') if username else None
                safe_password = quote(password, safe='') if password else None

                proxy_info: ProxyDict = {
                    'protocol': 'https',
                    'host': server["hostname"],
                    'port': port,
                    'username': safe_username, # Store URL-encoded version
                    'password': safe_password
                }
                formatted_proxies.append(proxy_info)
                count += 1

    logging.info(f"Found {count} servers with 'HTTP Proxy (SSL)' technology. Formatted with port {port}.")
    if not formatted_proxies:
         logging.warning(f"No servers matched required technology 'HTTP Proxy (SSL)' in {filepath}.")
    return formatted_proxies


# --- MODIFIED: fetch_paginated_data_for_id raises ProxyError on likely proxy failures ---
async def fetch_paginated_data_for_id(
    session: aiohttp.ClientSession,
    selected_proxy: Optional[ProxyDict], # Accepts one proxy dict (or None)
    item_id: str,
    # No semaphore needed here if controlled by batch size in main
) -> Tuple[str, str]: # Now guaranteed to return tuple on success, raises Exception on failure
    """
    Fetches all pages of sales data for a given item_id using the provided proxy.
    Raises ProxyError for connection/timeout issues likely related to the proxy.
    Raises other exceptions for target server errors (4xx/5xx) or data errors.
    Returns (item_id, json_data_string) on success.
    """
    target_url = TCGPLAYER_API_URL_TEMPLATE.format(item_id)
    all_sales_data = []
    current_offset = 0
    page_num = 1
    proxy_url_for_request: Optional[str] = None
    proxy_log_msg: str = "Direct"

    # --- Format the selected proxy ONCE ---
    if selected_proxy:
        protocol = selected_proxy.get('protocol', 'https')
        host = selected_proxy.get('host')
        port = selected_proxy.get('port')
        # Use the already URL-encoded credentials from loading step
        username = selected_proxy.get('username')
        password = selected_proxy.get('password')

        if host and port:
            user_pass = ""
            # Construct user:pass part carefully *without* re-encoding
            if username and password:
                user_pass = f"{username}:{password}@"
            elif username:
                user_pass = f"{username}@"
            proxy_url_for_request = f"{protocol}://{user_pass}{host}:{port}"
            proxy_log_msg = f"{protocol}://{host}:{port}"
        else:
            # This case shouldn't happen if load_proxies filters correctly, but safety first
             logging.warning(f"ID {item_id}: Invalid proxy data provided {selected_proxy}. Attempting direct connection.")
             proxy_url_for_request = None
             proxy_log_msg = "Invalid->Direct"

    logging.debug(f"ID {item_id}: Attempting fetch using proxy: {proxy_log_msg}")

    # --- Pagination Loop ---
    while True:
        current_payload = BASE_PAYLOAD.copy()
        current_payload["offset"] = current_offset

        try:
            logging.debug(f"ID {item_id} Page {page_num} Offset {current_offset}: Requesting...")
            async with session.post(
                target_url,
                params=REQUEST_QUERYSTRING,
                json=current_payload,
                headers=REQUEST_HEADERS,
                proxy=proxy_url_for_request,
                timeout=aiohttp.ClientTimeout(total=45) # Specify timeout clearly
            ) as response:
                # Check for specific HTTP errors first
                if response.status >= 400:
                     # Log server-side errors
                     err_text = await response.text()
                     logging.warning(f"ID {item_id} Page {page_num}: HTTP Error {response.status} from target server. Response: {err_text[:200]}")
                     # Raise generic Exception for non-proxy related HTTP errors
                     response.raise_for_status() # This will raise ClientResponseError

                response_json = await response.json() # Can raise JSONDecodeError or ContentTypeError

                # Process successful response
                if response_json and "data" in response_json:
                    page_data = response_json.get("data", [])
                    all_sales_data.extend(page_data)
                    logging.debug(f"ID {item_id} Page {page_num}: Fetched {len(page_data)} records.")

                    # Check pagination
                    if response_json.get("nextPage") == "Yes" and len(page_data) == REQUEST_LIMIT:
                        current_offset += REQUEST_LIMIT
                        page_num += 1
                        await asyncio.sleep(0.25) # Shorter delay between pages might be ok now
                    else:
                        logging.debug(f"ID {item_id}: Pagination end detected after page {page_num}.")
                        break # Finished pagination
                else:
                     # Success status but unexpected format
                     logging.error(f"ID {item_id} Page {page_num}: Unexpected JSON format (missing 'data'?). Response: {str(response_json)[:200]}")
                     # Treat as a non-proxy error, raise generic exception
                     raise ValueError("Unexpected JSON response format from target server")

        # --- Specific Error Handling -> Raise ProxyError where appropriate ---
        except (aiohttp.ClientConnectionError,
                aiohttp.ClientProxyConnectionError, # Specifically for proxy connection issues
                aiohttp.ServerDisconnectedError,
                asyncio.TimeoutError) as e:
            logging.warning(f"ID {item_id} Page {page_num}: Connection/Timeout error ({type(e).__name__}), likely proxy issue. Proxy: {proxy_log_msg}. Error: {e}")
            # Raise specific error to signal main loop to switch proxy
            raise ProxyError(f"Proxy connection/timeout issue for {item_id}: {e}") from e

        # --- Handle other errors (target server 4xx/5xx, JSON errors etc) ---
        except aiohttp.ClientResponseError as e: # Raised by response.raise_for_status()
             # Already logged above, re-raise to signal failure but not necessarily proxy fault
             logging.error(f"ID {item_id} Page {page_num}: Unrecoverable HTTP status {e.status}. Proxy: {proxy_log_msg}")
             raise # Re-raise the original ClientResponseError
        except (json.JSONDecodeError, aiohttp.ContentTypeError) as e:
             logging.error(f"ID {item_id} Page {page_num}: Failed to decode JSON response. Proxy: {proxy_log_msg}. Error: {e}")
             # Consider logging raw response here if helpful
             # Re-raise generic exception - not a proxy fault
             raise ValueError(f"JSON/Content Error for {item_id}: {e}") from e
        except Exception as e:
            # Catch any other unexpected errors
            logging.error(f"ID {item_id} Page {page_num}: Unexpected error during fetch: {e}. Proxy: {proxy_log_msg}", exc_info=True)
            # Re-raise generic Exception
            raise # Re-raise the original unknown exception

    # --- Success Case ---
    if all_sales_data:
        logging.debug(f"ID {item_id}: Successfully fetched {len(all_sales_data)} total records.")
        return item_id, json.dumps(all_sales_data)
    else:
        # Should only happen if first page was empty data
        logging.info(f"ID {item_id}: Fetch completed but no sales data found (e.g., empty first page).")
        return item_id, json.dumps([])

# --- Analyze Data (unchanged) ---
async def analyze_data(db_conn: aiosqlite.Connection):
    logging.info("Starting data analysis...")
    # ... (rest of the analysis function remains the same)
    try:
        record_count = 0;
        async with db_conn.execute(f"SELECT COUNT(*) FROM {DATA_TABLE_NAME}") as cursor: count_result = await cursor.fetchone(); record_count = count_result[0] if count_result else 0
        logging.info(f"Analysis: Found data for {record_count} IDs.")
        if record_count > 0:
             async with db_conn.execute(f"SELECT id, sales_data FROM {DATA_TABLE_NAME} LIMIT 1") as cursor: row = await cursor.fetchone()
             if row:
                 item_id, sales_data_str = row
                 try:
                     sales_list = json.loads(sales_data_str)
                     if sales_list: total_sales = len(sales_list); total_price = sum(sale.get('purchasePrice', 0) for sale in sales_list if isinstance(sale.get('purchasePrice'), (int, float))); average_price = total_price / total_sales if total_sales else 0; logging.info(f"Analysis (ID {item_id}): {total_sales} sales, avg ${average_price:.2f}")
                     else: logging.info(f"Analysis (ID {item_id}): Empty list.")
                 except Exception as e: logging.error(f"Analysis (ID {item_id}): Error - {e}")
        await asyncio.sleep(1); logging.info("Data analysis finished.")
    except Exception as e: logging.error(f"Analysis Error: {e}", exc_info=True)

# --- Main Orchestration (Modified for Sticky Proxy and Retries) ---
async def main():
    """Initializes services, loads proxies, runs fetch/store/analyze loop
       using a sticky proxy strategy with retries."""

    if not all([NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY]):
        logging.critical("Missing Supabase config. Exiting.")
        return
    supabase_client = get_supabase_client()
    if not supabase_client:
        logging.critical("Failed Supabase init. Exiting.")
        return

    logging.info(f"--- Starting Data Fetch Process ---")
    logging.info(f"Max Concurrent Requests: {MAX_CONCURRENT_REQUESTS}")
    logging.info(f"API Page Size Limit: {REQUEST_LIMIT}")
    logging.info(f"Max Retries per ID: {MAX_RETRIES_PER_ID}")

    # --- Load Proxies ---
    proxies: List[ProxyDict] = load_proxies_from_file(PROXY_FILE, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD)
    # Optional: Add direct connection as a fallback?
    # proxies.append(None) # Add None to represent direct connection at the end

    current_proxy_index: int = 0
    # Add a fallback to direct connection if no proxies are loaded
    use_direct_connection = not proxies
    if use_direct_connection:
        logging.warning("No proxies loaded or found. Attempting all requests directly.")

    # --- State Management ---
    all_ids = await fetch_ids_from_supabase(supabase_client)
    if not all_ids: logging.warning("No IDs fetched from Supabase."); return

    remaining_ids: Set[str] = set(all_ids)
    id_retry_counts: Dict[str, int] = {item_id: 0 for item_id in all_ids}
    failed_ids_permanently: Set[str] = set()
    successful_ids: Set[str] = set()
    store_tasks: List[asyncio.Task] = [] # Keep track of storage tasks

    start_time = asyncio.get_event_loop().time()

    try:
        async with aiosqlite.connect(DB_NAME) as db_conn:
            await setup_database(db_conn)

            # Use a single session
            async with aiohttp.ClientSession() as session:
                while remaining_ids:
                    if not use_direct_connection and current_proxy_index >= len(proxies):
                         logging.error(f"Cycled through all {len(proxies)} proxies. {len(remaining_ids)} IDs still failing.")
                         # Option: Try direct connection as a last resort
                         # if not use_direct_connection and None not in proxies: # Check if direct was already tried
                         #     logging.warning("Attempting direct connection as last resort...")
                         #     use_direct_connection = True # Set flag to use None as proxy
                         # else:
                         failed_ids_permanently.update(remaining_ids)
                         remaining_ids.clear() # Stop the loop
                         break

                    # Determine proxy for this batch
                    if use_direct_connection:
                        current_proxy = None
                        proxy_log_msg = "Direct Connection"
                    else:
                        current_proxy = proxies[current_proxy_index]
                        proxy_host = current_proxy.get('host', 'N/A') if current_proxy else 'N/A'
                        proxy_log_msg = f"Proxy #{current_proxy_index} ({proxy_host})"

                    logging.info(f"--- Starting Batch | IDs Remaining: {len(remaining_ids)} | Using: {proxy_log_msg} ---")

                    # Create batch of tasks
                    batch_tasks = []
                    ids_in_batch = []
                    # Take a slice of remaining IDs up to MAX_CONCURRENT_REQUESTS
                    ids_to_process_this_batch = list(remaining_ids)[:MAX_CONCURRENT_REQUESTS]

                    for item_id in ids_to_process_this_batch:
                        # Create task without semaphore (concurrency managed by batch size)
                        task = asyncio.create_task(
                            fetch_paginated_data_for_id(session, current_proxy, item_id),
                            name=f"Fetch-{item_id}-{proxy_log_msg[:10]}"
                        )
                        batch_tasks.append(task)
                        ids_in_batch.append(item_id)

                    # Run the batch
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                    # Process results for this batch
                    proxy_failed_this_batch = False
                    ids_succeeded_this_batch = set()
                    ids_failed_non_proxy_this_batch = set()

                    for item_id, result in zip(ids_in_batch, results):
                        if isinstance(result, ProxyError):
                            proxy_failed_this_batch = True
                            logging.warning(f"ID {item_id}: Failed with ProxyError using {proxy_log_msg}. Will retry.")
                            # ID remains in remaining_ids
                        elif isinstance(result, Exception):
                            logging.error(f"ID {item_id}: Failed with non-proxy error ({type(result).__name__}: {result}). Proxy: {proxy_log_msg}")
                            # Increment retry count, decide if permanently failed
                            id_retry_counts[item_id] += 1
                            if id_retry_counts[item_id] >= MAX_RETRIES_PER_ID:
                                logging.error(f"ID {item_id}: Exceeded max retries ({MAX_RETRIES_PER_ID}). Marking as failed.")
                                failed_ids_permanently.add(item_id)
                                ids_failed_non_proxy_this_batch.add(item_id) # Remove from remaining
                            else:
                                logging.info(f"ID {item_id}: Will retry (Attempt {id_retry_counts[item_id]}/{MAX_RETRIES_PER_ID}).")
                                # ID remains in remaining_ids for now unless retry limit hit
                        elif isinstance(result, tuple) and len(result) == 2:
                            # Success case: (item_id, json_data_string)
                            fetched_id, json_data = result
                            if fetched_id == item_id:
                                logging.debug(f"ID {item_id}: Successfully fetched.")
                                ids_succeeded_this_batch.add(item_id)
                                successful_ids.add(item_id) # Add to overall success set
                                # Create storage task immediately
                                store_tasks.append(asyncio.create_task(store_data(db_conn, item_id, json_data)))
                            else: # Should not happen
                                 logging.error(f"ID Mismatch! Expected {item_id}, got {fetched_id}. Treating as failure.")
                                 failed_ids_permanently.add(item_id) # Assume unrecoverable error
                                 ids_failed_non_proxy_this_batch.add(item_id) # Remove from remaining
                        else: # Unexpected result format
                            logging.error(f"ID {item_id}: Received unexpected result format: {type(result)}. Treating as failure.")
                            failed_ids_permanently.add(item_id)
                            ids_failed_non_proxy_this_batch.add(item_id) # Remove from remaining

                    # --- Update State After Batch ---
                    # Remove succeeded and permanently failed IDs from the set to be processed
                    remaining_ids -= ids_succeeded_this_batch
                    remaining_ids -= ids_failed_non_proxy_this_batch


                    # If proxy failed, move to the next one for the NEXT batch
                    if proxy_failed_this_batch and not use_direct_connection:
                        logging.warning(f"Proxy {proxy_log_msg} failed for one or more IDs in the batch. Switching proxy.")
                        current_proxy_index += 1
                    elif proxy_failed_this_batch and use_direct_connection:
                        logging.error("Direct connection failed. No more options.")
                        failed_ids_permanently.update(remaining_ids) # Mark all remaining as failed
                        remaining_ids.clear() # Stop the loop


                    await asyncio.sleep(0.5) # Small delay between batches


            # --- Wait for any outstanding storage tasks ---
            if store_tasks:
                logging.info(f"Waiting for {len(store_tasks)} storage tasks to complete...")
                await asyncio.gather(*store_tasks, return_exceptions=True) # Log storage errors if needed
                logging.info("Storage tasks finished.")

            # --- Final Summary ---
            logging.info("--- Processing Complete ---")
            logging.info(f"Total Initial IDs: {len(all_ids)}")
            logging.info(f"Successfully Processed & Stored: {len(successful_ids)}")
            logging.info(f"Permanently Failed IDs: {len(failed_ids_permanently)}")
            if failed_ids_permanently:
                 logging.warning(f"Failed IDs: {', '.join(sorted(list(failed_ids_permanently)))}")


            # --- Analysis Phase ---
            await analyze_data(db_conn)

    except aiosqlite.Error as e:
        logging.critical(f"Critical SQLite Error: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in main loop: {e}", exc_info=True)
    finally:
        end_time = asyncio.get_event_loop().time()
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")


# --- Script Entry Point ---
if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())