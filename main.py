# main.py
import asyncio
import aiohttp
import logging
from datetime import date, timedelta, datetime
from typing import List, Dict, Set, Optional, Tuple  # Added Tuple
import requests  # For synchronous pre-flight proxy test
import sys       # For exiting early on pre-flight failure

# Import local modules
import config                  # Project config
import proxies                 # Proxy loading
import database                # DB interactions
import fetcher                 # Data fetching logic (includes ProxyError)
from fetcher import ProxyError # Import custom exception explicitly
import analysis                # Analysis functions
import aiosqlite
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# --- Helper: Fetch Target IDs from Supabase (Paginated - Unchanged) ---
async def get_target_ids(supabase: Client) -> List[str]:
    """Fetches tcgplayer_ids from Supabase, filtering by TARGET_SETS in config,
       using pagination to avoid timeouts."""
    # ... (Keep the existing implementation from your previous file) ...
    if not supabase: return []
    select_query = f"{config.SUPABASE_COLUMN_ID}" # Column to fetch
    all_valid_ids: set[str] = set() # Use a set for efficient deduplication
    current_offset = 0
    ID_FETCH_PAGE_SIZE = 10000 # Fetch IDs in batches, adjust as needed

    logging.info(f"Fetching target IDs (column: {config.SUPABASE_COLUMN_ID}) "
                 f"from table '{config.SUPABASE_TABLE_CARDS}' "
                 f"in batches of {ID_FETCH_PAGE_SIZE}.")
    if config.TARGET_SETS:
        logging.info(f"Filtering by set column '{config.SUPABASE_COLUMN_SET}' for sets: {config.TARGET_SETS}")
    else:
        logging.warning("No TARGET_SETS defined in config. Fetching all IDs!")

    try:
        loop = asyncio.get_running_loop()
        while True:
            logging.debug(f"Fetching ID page starting at offset {current_offset}...")
            query_builder = supabase.table(config.SUPABASE_TABLE_CARDS)\
                                    .select(select_query, count='exact')\
                                    .limit(ID_FETCH_PAGE_SIZE)\
                                    .offset(current_offset)
            if config.TARGET_SETS:
                query_builder = query_builder.in_(config.SUPABASE_COLUMN_SET, config.TARGET_SETS)
            response = await loop.run_in_executor(None, query_builder.execute)

            if not hasattr(response, 'data'):
                logging.error(f"Supabase response missing 'data' at offset {current_offset}. Response: {response}")
                break
            if response.data:
                ids_on_page = [str(item[config.SUPABASE_COLUMN_ID]) for item in response.data
                               if config.SUPABASE_COLUMN_ID in item and item[config.SUPABASE_COLUMN_ID]]
                valid_ids_on_page = {id_ for id_ in ids_on_page if id_.strip()}
                if not valid_ids_on_page: break
                all_valid_ids.update(valid_ids_on_page)
                logging.info(f"Fetched {len(valid_ids_on_page)} IDs page offset {current_offset}. Total unique: {len(all_valid_ids)}")
                if len(response.data) < ID_FETCH_PAGE_SIZE: break
                else:
                    current_offset += ID_FETCH_PAGE_SIZE
                    await asyncio.sleep(0.2) # Slightly increased delay
            else: break
    except Exception as e:
        if "APIError" in str(type(e)) and hasattr(e, 'json') and callable(e.json):
             try: logging.error(f"Supabase API Error: {e.json()}", exc_info=False)
             except: logging.error(f"Supabase API Error (No JSON): {e}", exc_info=False)
        elif "APIError" in str(type(e)): logging.error(f"Supabase API Error: {e}", exc_info=False)
        else: logging.error(f"Unexpected Error fetching IDs: {e}", exc_info=True)
        return []
    final_id_list = sorted(list(all_valid_ids))
    logging.info(f"Finished fetching IDs. Total unique retrieved: {len(final_id_list)}")
    return final_id_list


# --- Main Orchestration Logic ---
async def run_monitor():
    """Main function to orchestrate fetching, storing, and analyzing with pre-flight checks and chunking."""
    logging.info("--- TCG Market Monitor Starting ---")
    start_time_main = datetime.now()
    db_conn: Optional[aiosqlite.Connection] = None # Define db_conn in outer scope

    # ======================================
    # ===        PRE-FLIGHT CHECKS       ===
    # ======================================
    logging.info("--- Performing Pre-flight Checks ---")
    checks_passed = True
    supabase_client: Optional[Client] = None # Define supabase_client in outer scope

    # 1. Supabase Client Init & Check
    if config.SUPABASE_URL and config.SUPABASE_ANON_KEY:
        try:
           supabase_client = create_client(config.SUPABASE_URL, config.SUPABASE_ANON_KEY)
           logging.info("Supabase client initialized.")
           # Simple test query
           loop = asyncio.get_running_loop()
           test_query = supabase_client.table(config.SUPABASE_TABLE_CARDS).select(config.SUPABASE_COLUMN_ID, count='exact').limit(1)
           test_response = await loop.run_in_executor(None, test_query.execute)
           # Check count attribute if available, otherwise just check if call succeeded
           if hasattr(test_response, 'count'):
                logging.info(f"Supabase connection check: OK (Test query returned count: {test_response.count})")
           elif hasattr(test_response, 'data'):
                logging.info("Supabase connection check: OK (Test query successful)")
           else:
               logging.warning(f"Supabase connection check: Test query response structure unknown: {test_response}") # Warn but may continue

        except Exception as e:
            logging.error(f"Supabase initialization or connection check failed: {e}", exc_info=False)
            checks_passed = False
            supabase_client = None # Ensure client is None on failure
    else:
        logging.warning("Supabase URL or Key not configured. Cannot perform Supabase check or fetch target IDs.")
        # If Supabase is *required* for target IDs, uncomment the next lines:
        # checks_passed = False
        # logging.critical("Supabase configuration missing and is required. Aborting.")

    # 2. Database Connection & Check
    try:
        db_conn = await database.get_db_connection()
        # Simple check: Can we access schema info?
        await db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (config.DB_TABLE_SALES,))
        logging.info(f"Database '{config.SQLITE_DB_NAME}' connection check: OK")
    except Exception as e:
        logging.error(f"Database connection/check failed for '{config.SQLITE_DB_NAME}': {e}", exc_info=True)
        checks_passed = False
        if db_conn: await db_conn.close() # Close if opened but check failed
        db_conn = None # Ensure db_conn is None if check fails

    # 3. Proxy Test (First Available Proxy Only)
    loaded_proxies = proxies.load_proxies() # Load proxies early for test
    if config.USE_PROXIES and loaded_proxies:
        first_proxy_config = loaded_proxies[0]
        proxy_url_test = fetcher.format_proxy_url(first_proxy_config)
        if proxy_url_test:
            test_target_url = "https://httpbin.org/get" # Reliable endpoint for testing connectivity/auth
            sync_proxies = {'http': proxy_url_test, 'https': proxy_url_test}
            logging.info(f"Testing first proxy ({first_proxy_config.get('host')}, {first_proxy_config.get('location','N/A')}) to {test_target_url}...")
            try:
                # Use synchronous requests library for simple check
                response = requests.get(test_target_url, proxies=sync_proxies, timeout=15, allow_redirects=False)
                response.raise_for_status() # Raises HTTPError for 4xx/5xx (will catch 407)
                logging.info(f"Proxy test connection: OK (Status: {response.status_code})")
            except requests.exceptions.HTTPError as e:
                 logging.error(f"Proxy test failed (HTTP Error {e.response.status_code}): Check credentials/proxy status. Error: {e}", exc_info=False)
                 checks_passed = False
            except requests.exceptions.ProxyError as e:
                logging.error(f"Proxy test failed (ProxyError - check network/proxy config): {e}", exc_info=False)
                checks_passed = False
            except requests.exceptions.Timeout:
                logging.error("Proxy test failed (Timeout).")
                checks_passed = False
            except requests.exceptions.RequestException as e:
                 logging.error(f"Proxy test failed (Request Error: {e})", exc_info=False)
                 checks_passed = False
        else:
             logging.warning("Could not format first proxy URL for testing.")
    elif config.USE_PROXIES:
        logging.warning("Proxy usage enabled but no proxies loaded/found. Skipping proxy test.")

    # --- Exit if Checks Failed ---
    if not checks_passed:
        logging.critical("!!! One or more pre-flight checks failed. Aborting run. !!!")
        if db_conn: await db_conn.close()
        sys.exit(1) # Exit script with non-zero code

    logging.info("--- Pre-flight checks passed ---")

    # --- Proceed with DB Setup (connection check already passed) ---
    try:
        await database.setup_schema(db_conn)
    except Exception as e:
         logging.critical(f"Failed to set up database schema: {e}. Aborting.", exc_info=True)
         await db_conn.close()
         sys.exit(1)

    # Use the proxies loaded during the check
    proxy_list = loaded_proxies if config.USE_PROXIES and loaded_proxies else [None]

    # --- Fetch Target IDs (client check already passed) ---
    target_ids = await get_target_ids(supabase_client) if supabase_client else []
    if not target_ids:
        logging.warning("No target IDs found to process.")
        # Decide if you want to proceed to analysis or exit
        # If analysis depends ONLY on previously stored data, you might continue.
        # For now, let's assume fetching is primary goal if target_ids was expected.
        # Consider exiting if no target IDs found unless specific need to run analysis anyway
        # logging.info("Exiting as no target IDs were found.")
        # await db_conn.close()
        # return # Optional early exit

    # ======================================
    # ===     CHUNK PROCESSING LOOP      ===
    # ======================================
    # --- Global State (Maintained Across Chunks) ---
    current_proxy_index = 0 # Maintain proxy index across chunks
    global_id_retry_counts: Dict[str, int] = {item_id: 0 for item_id in target_ids} # Track retries globally
    global_failed_ids_permanently: Set[str] = set()
    global_successful_fetch_ids: Set[str] = set()
    global_total_inserted_count = 0
    global_total_store_errors = 0

    # --- Chunking Setup ---
    CHUNK_SIZE = config.PROCESSING_CHUNK_SIZE if hasattr(config, 'PROCESSING_CHUNK_SIZE') else 100 # Define in config.py or default
    all_target_ids_list = list(target_ids) # Work with a list for slicing
    num_chunks = (len(all_target_ids_list) + CHUNK_SIZE - 1) // CHUNK_SIZE if CHUNK_SIZE > 0 else (1 if all_target_ids_list else 0)

    if not all_target_ids_list:
         logging.info("No target IDs to process in chunks.")
         num_chunks = 0 # Ensure loop doesn't run if list is empty

    logging.info(f"Preparing to process {len(all_target_ids_list)} IDs in {num_chunks} chunks of size {CHUNK_SIZE}.")

    # --- Main Chunk Loop ---
    # Use one session for efficiency across chunks
    async with aiohttp.ClientSession() as session:
        for i in range(num_chunks):
            chunk_start_index = i * CHUNK_SIZE
            chunk_end_index = chunk_start_index + CHUNK_SIZE
            current_chunk_ids = all_target_ids_list[chunk_start_index:chunk_end_index]

            logging.info(f"--- Processing Chunk {i+1}/{num_chunks} ({len(current_chunk_ids)} IDs) ---")

            # --- Fetching & Storing Logic for the Current Chunk ---
            remaining_ids_chunk: Set[str] = set(current_chunk_ids)
            # We don't need separate retry/failed state for chunk, update global state directly
            successful_fetch_ids_chunk: Set[str] = set()
            store_tasks_chunk: List[asyncio.Task] = []
            chunk_fetch_start_time = datetime.now()

            while remaining_ids_chunk:
                if current_proxy_index >= len(proxy_list):
                     logging.error(f"[Chunk {i+1}] Exhausted all proxy/direct options. {len(remaining_ids_chunk)} IDs in this chunk failed.")
                     global_failed_ids_permanently.update(remaining_ids_chunk) # Mark remaining in chunk as failed globally
                     remaining_ids_chunk.clear() # Stop processing this chunk
                     break # Exit inner while loop

                # Determine proxy for this batch (using global index)
                current_proxy_config = proxy_list[current_proxy_index]
                proxy_host_info = "Direct Connection" if current_proxy_config is None else current_proxy_config.get('host', 'N/A')
                proxy_log_msg = f"Proxy #{current_proxy_index} ({proxy_host_info})"

                logging.info(f"[Chunk {i+1}] Starting Batch | IDs Rem: {len(remaining_ids_chunk)} | Using: {proxy_log_msg} ---")

                # Create batch of tasks from chunk's remaining IDs
                tasks_in_batch = []
                ids_in_batch = list(remaining_ids_chunk)[:config.MAX_CONCURRENT_REQUESTS]

                for item_id in ids_in_batch:
                    task = asyncio.create_task(
                        fetcher.fetch_sales_for_id(session, current_proxy_config, item_id),
                        name=f"Fetch-{item_id}-{proxy_host_info[:10]}"
                    )
                    tasks_in_batch.append(task)

                # Run the batch
                results = await asyncio.gather(*tasks_in_batch, return_exceptions=True)

                # Process results for this batch
                proxy_failed_this_batch = False
                # Don't need chunk-local success/fail sets, update global ones

                for item_id, result in zip(ids_in_batch, results):
                    if item_id in global_failed_ids_permanently: continue # Already marked as perm failed

                    if isinstance(result, ProxyError):
                         proxy_failed_this_batch = True
                         logging.warning(f"[Chunk {i+1}] ID {item_id}: ProxyError ({result}). Will retry.")
                    elif isinstance(result, Exception):
                        logging.error(f"[Chunk {i+1}] ID {item_id}: Failed non-proxy ({type(result).__name__}: {result}).")
                        global_id_retry_counts[item_id] += 1
                        if global_id_retry_counts[item_id] >= config.MAX_RETRIES_PER_ID:
                            logging.error(f"[Chunk {i+1}] ID {item_id}: Max retries reached. Marked failed permanently.")
                            global_failed_ids_permanently.add(item_id)
                            remaining_ids_chunk.discard(item_id) # Remove from chunk processing
                        else:
                            logging.info(f"[Chunk {i+1}] ID {item_id}: Will retry non-proxy (Attempt {global_id_retry_counts[item_id]}).")
                            # Stays in remaining_ids_chunk
                    elif isinstance(result, list):
                        logging.debug(f"[Chunk {i+1}] ID {item_id}: Fetch success ({len(result)} sales).")
                        global_successful_fetch_ids.add(item_id)
                        remaining_ids_chunk.discard(item_id) # Remove from chunk processing
                        if result:
                           store_tasks_chunk.append(asyncio.create_task(
                               database.store_sales_batch(db_conn, item_id, result),
                               name=f"Store-{item_id}"
                           ))
                    else:
                        logging.error(f"[Chunk {i+1}] ID {item_id}: Bad result type {type(result)}. Marked failed permanently.")
                        global_failed_ids_permanently.add(item_id)
                        remaining_ids_chunk.discard(item_id) # Remove from chunk

                # --- Decide Next Action for Proxy (Maintained Globally) ---
                if proxy_failed_this_batch:
                    logging.warning(f"[Chunk {i+1}] {proxy_log_msg} flagged. Switching global proxy index.")
                    current_proxy_index += 1
                elif not remaining_ids_chunk:
                     logging.info(f"[Chunk {i+1}] All processable IDs in chunk finished.")
                     break # Exit inner while loop

                await asyncio.sleep(0.1) # Delay between batches WITHIN a chunk

            # --- Post-Chunk Processing ---
            chunk_fetch_end_time = datetime.now()
            logging.info(f"[Chunk {i+1}] Fetching phase finished in {chunk_fetch_end_time - chunk_fetch_start_time}.")

            # Wait for Storage *for this chunk* & Check Results
            if store_tasks_chunk:
                logging.info(f"[Chunk {i+1}] Waiting for {len(store_tasks_chunk)} storage tasks...")
                storage_results_chunk = await asyncio.gather(*store_tasks_chunk, return_exceptions=True)
                inserted_chunk = 0
                errors_chunk = 0
                for k, res in enumerate(storage_results_chunk):
                    if isinstance(res, Exception):
                        errors_chunk += 1
                        # Log details, maybe using task name if python > 3.8? store_tasks_chunk[k].get_name()
                        logging.error(f"[Chunk {i+1}] Storage task failed: {res}", exc_info=False)
                    elif isinstance(res, int):
                        inserted_chunk += res
                    else:
                        logging.warning(f"[Chunk {i+1}] Unexpected storage result type: {type(res)}")
                logging.info(f"[Chunk {i+1}] Storage done. New records: {inserted_chunk}. Errors: {errors_chunk}.")
                global_total_inserted_count += inserted_chunk
                global_total_store_errors += errors_chunk
                if errors_chunk > 0:
                    logging.critical(f"[Chunk {i+1}] CRITICAL: {errors_chunk} Storage Errors detected! Check DB/Data.")
                    # Add logic here: Maybe stop if too many storage errors?
                    # if errors_chunk / len(store_tasks_chunk) > 0.5: # Example threshold
                    #     logging.critical("High storage error rate. Aborting further processing.")
                    #     if db_conn: await db_conn.close()
                    #     sys.exit(1)

            else:
                logging.info(f"[Chunk {i+1}] No storage tasks generated for this chunk.")

            # Optional: Quick DB Date Check (already defined above)
            # min_d_qc, max_d_qc = await find_sales_date_range(db_conn)
            # logging.info(f"[Chunk {i+1}] Quick Check: DB date range {min_d_qc} to {max_d_qc}")

            logging.info(f"--- Finished Processing Chunk {i+1}/{num_chunks} ---")
            # Optional delay between chunks
            # await asyncio.sleep(1.0)

    # ======================================
    # ===     FINAL ANALYSIS PHASE       ===
    # ======================================
    logging.info(f"--- All chunks processed. Starting Final Analysis Phase ---")
    logging.info(f"Overall Fetch Summary: Success: {len(global_successful_fetch_ids)}, Failed: {len(global_failed_ids_permanently)}")
    logging.info(f"Overall Storage Summary: Total New Records: {global_total_inserted_count}, Errors: {global_total_store_errors}")


    # Nested function to find date range (needed if defined inside run_monitor)
    async def find_sales_date_range(conn: aiosqlite.Connection) -> Tuple[Optional[date], Optional[date]]:
        # ... (Implementation is the same as previous answer) ...
        query = f"SELECT MIN(date(order_date)), MAX(date(order_date)) FROM {config.DB_TABLE_SALES}"
        try:
            async with conn.execute(query) as cursor:
                result = await cursor.fetchone()
                if result and result[0] and result[1]:
                    min_d = date.fromisoformat(result[0])
                    max_d = date.fromisoformat(result[1])
                    return min_d, max_d
            logging.warning("No date range found in sales table for final analysis.")
            return None, None
        except Exception as e:
            logging.error(f"Error finding final sales date range: {e}")
            return None, None

    min_sales_date, max_sales_date = await find_sales_date_range(db_conn)

    if min_sales_date and max_sales_date:
        analysis_latest_date = max_sales_date
        analysis_earliest_needed = min(min_sales_date, analysis_latest_date - timedelta(days=config.ANALYSIS_BASELINE_DAYS + 2))

        logging.info(f"Final analysis: Ensuring metrics exist from {analysis_earliest_needed} to {analysis_latest_date}...")
        current_calc_date = analysis_earliest_needed
        while current_calc_date <= analysis_latest_date:
            await analysis.calculate_daily_metrics(db_conn, current_calc_date) # Ensure metrics are up-to-date
            current_calc_date += timedelta(days=1)

        logging.info(f"Final analysis: Generating insights for {analysis_latest_date}...")
        all_insights = await analysis.run_analysis_for_date(db_conn, analysis_latest_date)

        # --- Reporting Phase ---
        if all_insights:
            logging.warning(f"--- Final Insights Report ({len(all_insights)} items) for {analysis_latest_date} ---")
            # ... (Keep the existing detailed reporting section using format_pct helper) ...
            def format_pct(value):
                if isinstance(value, (int, float)) and value != float('inf'): return f"{value:+7.1f}%"
                elif value == 'inf': return " (+inf%)"
                else: return " ( N/A )"
            top_volume_movers = sorted([i for i in all_insights if isinstance(i.get('vol_change_pct'),(int, float))and i.get('volume',0)>=3],key=lambda x: x.get('vol_change_pct',-float('inf')),reverse=True)[:15]
            top_price_movers = sorted([i for i in all_insights if isinstance(i.get('avg_p_change_pct'),(int, float))and i.get('avg_price',0)>=1.00],key=lambda x: x.get('avg_p_change_pct',-float('inf')),reverse=True)[:15]
            top_abs_volume = sorted([i for i in all_insights if i.get('volume',0)>=5],key=lambda x: x.get('volume',0),reverse=True)[:15]
            logging.warning("--- Top Volume Movers (% Change vs Prev Day | Vol >= 3) ---")
            if top_volume_movers:
                for item in top_volume_movers: vol_pct_str = format_pct(item.get('vol_change_pct')); logging.warning(f"  ID: {item['tcgplayer_id']:<10} Vol: {item['volume']:<4} {vol_pct_str} AvgP: ${item['avg_price']:.2f}")
            else: logging.warning("  (No cards met Volume % Change criteria)")
            logging.warning("--- Top Avg Price Movers (% Change vs Prev Day | AvgP >= $1.00) ---")
            if top_price_movers:
                for item in top_price_movers: avg_p_pct_str = format_pct(item.get('avg_p_change_pct')); logging.warning(f"  ID: {item['tcgplayer_id']:<10} AvgP: ${item['avg_price']:<7.2f} {avg_p_pct_str} Vol: {item['volume']}")
            else: logging.warning("  (No cards met Avg Price % Change criteria)")
            logging.warning("--- Highest Volume Cards (Latest Day | Vol >= 5) ---")
            if top_abs_volume:
                 for item in top_abs_volume: change_str = format_pct(item.get('avg_p_change_pct')); logging.warning(f"  ID: {item['tcgplayer_id']:<10} Vol: {item['volume']:<4} AvgP: ${item['avg_price']:.2f} Change:{change_str}")
            else: logging.warning("  (No cards met Highest Volume criteria)")
        else:
             logging.info(f"Final analysis for {analysis_latest_date} did not generate significant insights.")

    else:
        logging.warning("No sales data found in the database after all chunks. Cannot perform final analysis.")


    # --- Cleanup ---
    if db_conn:
       await db_conn.close()
       logging.info("Database connection closed.")
    else:
       logging.warning("Database connection was not available for cleanup.")

    end_time_main = datetime.now()
    logging.info(f"--- TCG Market Monitor Finished | Total Time: {end_time_main - start_time_main} ---")


if __name__ == "__main__":
    # Add a default chunk size to config if it doesn't exist (or define directly)
    if not hasattr(config, 'PROCESSING_CHUNK_SIZE'):
         setattr(config, 'PROCESSING_CHUNK_SIZE', 100) # Example default
    asyncio.run(run_monitor())