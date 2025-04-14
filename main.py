# main.py
import asyncio
import aiohttp
import logging
from datetime import date, timedelta, datetime
from typing import List, Dict, Set, Optional
from supabase import create_client, Client
import config                  # Project config
import proxies                 # Proxy loading
import database                # DB interactions
import fetcher                 # Data fetching logic (includes ProxyError)
from fetcher import ProxyError # Import custom exception explicitly
import analysis                # Analysis functions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(module)s] %(message)s')

# --- Helper: Fetch Target IDs from Supabase ---
async def get_target_ids(supabase: Client) -> List[str]:
    """Fetches tcgplayer_ids from Supabase, filtering by TARGET_SETS in config."""
    if not supabase: return []
    select_query = f"{config.SUPABASE_COLUMN_ID}" # Always select the ID column

    try:
        loop = asyncio.get_running_loop()
        query_builder = supabase.table(config.SUPABASE_TABLE_CARDS).select(select_query)

        # Apply filtering if target sets are defined
        if config.TARGET_SETS:
            logging.info(f"Fetching IDs only for sets: {config.TARGET_SETS}")
            query_builder = query_builder.in_(config.SUPABASE_COLUMN_SET, config.TARGET_SETS)
        else:
            logging.warning("No TARGET_SETS defined in config. Fetching all IDs - potentially very slow!")
            # Optionally add a limit here for safety? .limit(1000)

        response = await loop.run_in_executor(None, query_builder.execute)

        if response and response.data:
            ids = [str(item[config.SUPABASE_COLUMN_ID]) for item in response.data
                   if config.SUPABASE_COLUMN_ID in item and item[config.SUPABASE_COLUMN_ID]]
            valid_ids = list(set(id_ for id_ in ids if id_.strip())) # Deduplicate and remove empty
            logging.info(f"Fetched {len(valid_ids)} unique target IDs from Supabase.")
            return valid_ids
        else:
            logging.warning(f"No data returned from Supabase for target IDs. Response: {response}")
            return []
    except Exception as e:
        logging.error(f"Error fetching target IDs from Supabase: {e}", exc_info=True)
        return []

# --- Main Orchestration Logic ---
async def run_monitor():
    """Main function to orchestrate fetching, storing, and analyzing."""
    logging.info("--- TCG Market Monitor Starting ---")
    start_time_main = datetime.now()

    # --- Initialize Supabase Client ---
    supabase_client: Optional[Client] = None
    if config.SUPABASE_URL and config.SUPABASE_ANON_KEY:
        try:
           supabase_client = create_client(config.SUPABASE_URL, config.SUPABASE_ANON_KEY)
           logging.info("Supabase client initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Supabase client: {e}")
            # Decide if you can continue without Supabase or need to exit
            # return
    else:
        logging.warning("Supabase URL or Key not configured. Cannot fetch target IDs.")
        return # Exit if Supabase is required

    # --- Initialize Database ---
    db_conn = await database.get_db_connection()
    await database.setup_schema(db_conn)

    # --- Load Proxies ---
    loaded_proxies = proxies.load_proxies()
    proxy_list = loaded_proxies if config.USE_PROXIES and loaded_proxies else [None] # Use list with None for direct conn/fallback

    # --- Fetch Target IDs ---
    target_ids = await get_target_ids(supabase_client) if supabase_client else []
    if not target_ids:
        logging.warning("No target IDs found. Exiting.")
        await db_conn.close()
        return

    # --- Fetching Loop (Sticky Proxy Logic) ---
    logging.info(f"Starting fetch process for {len(target_ids)} target IDs...")
    current_proxy_index = 0
    remaining_ids: Set[str] = set(target_ids)
    id_retry_counts: Dict[str, int] = {item_id: 0 for item_id in target_ids}
    failed_ids_permanently: Set[str] = set()
    successful_fetch_ids: Set[str] = set()
    fetch_tasks = [] # To hold asyncio tasks for gather

    # Reusable AIOHTTP session
    async with aiohttp.ClientSession() as session:
        while remaining_ids:
            if current_proxy_index >= len(proxy_list):
                 logging.error(f"Exhausted all {len(proxy_list)} proxy/direct options. {len(remaining_ids)} IDs could not be fetched.")
                 failed_ids_permanently.update(remaining_ids)
                 remaining_ids.clear()
                 break

            current_proxy_config = proxy_list[current_proxy_index]
            proxy_host_info = "Direct" if current_proxy_config is None else current_proxy_config.get('host', 'N/A')
            proxy_log_msg = f"Proxy #{current_proxy_index} ({proxy_host_info})"

            logging.info(f"--- Starting Fetch Batch | IDs Remaining: {len(remaining_ids)} | Using: {proxy_log_msg} ---")

            tasks_in_batch = []
            ids_in_batch = list(remaining_ids)[:config.MAX_CONCURRENT_REQUESTS]

            for item_id in ids_in_batch:
                 task = asyncio.create_task(
                    fetcher.fetch_sales_for_id(session, current_proxy_config, item_id),
                    name=f"Fetch-{item_id}-{proxy_host_info[:10]}"
                 )
                 tasks_in_batch.append(task)

            results = await asyncio.gather(*tasks_in_batch, return_exceptions=True)

            # Process results
            proxy_failed_this_batch = False
            ids_succeeded_this_batch = set()
            ids_failed_non_proxy_this_batch = set()

            for item_id, result in zip(ids_in_batch, results):
                if isinstance(result, ProxyError):
                     proxy_failed_this_batch = True
                     logging.warning(f"ID {item_id}: ProxyError ({result}). Will retry.")
                     # Resetting retry count here could be an option, but maybe not needed
                     # id_retry_counts[item_id] = 0
                elif isinstance(result, Exception):
                    logging.error(f"ID {item_id}: Non-proxy error ({type(result).__name__}: {result}).")
                    id_retry_counts[item_id] += 1
                    if id_retry_counts[item_id] >= config.MAX_RETRIES_PER_ID:
                        logging.error(f"ID {item_id}: Max retries reached. Marking failed.")
                        failed_ids_permanently.add(item_id)
                        ids_failed_non_proxy_this_batch.add(item_id)
                    else:
                        logging.info(f"ID {item_id}: Will retry non-proxy error (Attempt {id_retry_counts[item_id]}).")
                elif isinstance(result, list): # Success returns a list of sales dicts
                    logging.debug(f"ID {item_id}: Successfully fetched {len(result)} sales.")
                    ids_succeeded_this_batch.add(item_id)
                    successful_fetch_ids.add(item_id)
                    # Schedule storage task (fire and forget for now, gather later maybe?)
                    if result: # Only store if there's actual data
                       asyncio.create_task(database.store_sales_batch(db_conn, item_id, result))
                else: # Unexpected success result type
                    logging.error(f"ID {item_id}: Unexpected success result type {type(result)}. Marking failed.")
                    failed_ids_permanently.add(item_id)
                    ids_failed_non_proxy_this_batch.add(item_id)

            # Update state
            remaining_ids -= ids_succeeded_this_batch
            remaining_ids -= ids_failed_non_proxy_this_batch

            if proxy_failed_this_batch:
                logging.warning(f"{proxy_log_msg} flagged with ProxyError. Switching proxy.")
                current_proxy_index += 1
            # elif not remaining_ids: break # No need, while condition handles it

            await asyncio.sleep(3.0) # Delay between batches

    logging.info("--- Fetching phase complete ---")
    logging.info(f"Successfully fetched data for {len(successful_fetch_ids)} IDs.")
    if failed_ids_permanently:
        logging.warning(f"{len(failed_ids_permanently)} IDs failed permanently after retries.")
        # Log first few failed IDs for debugging
        failed_sample = sorted(list(failed_ids_permanently))[:10]
        logging.warning(f"Sample failed IDs: {failed_sample}{'...' if len(failed_ids_permanently) > 10 else ''}")


    # --- Analysis Phase ---
    # Optional: Wait for all storage tasks to complete before analysis
    # Gather outstanding tasks if needed: await asyncio.gather(*all_store_tasks)

    # Calculate metrics for today (or yesterday if running early morning)
    analysis_target_date = date.today() - timedelta(days=1) # Analyze yesterday's complete data
    logging.info(f"--- Starting Analysis Phase for {analysis_target_date} ---")
    await analysis.calculate_daily_metrics(db_conn, analysis_target_date)

    # Detect spikes based on newly calculated metrics
    detected_spikes = await analysis.detect_spikes(db_conn, analysis_target_date)

    # --- Reporting ---
    if detected_spikes:
        logging.warning(f"--- Detected {len(detected_spikes)} Potential Spikes for {analysis_target_date} ---")
        # Add reporting logic here (e.g., print, save to file, send notification)
        for spike in detected_spikes:
             logging.warning(f"  - ID: {spike['tcgplayer_id']}, "
                             f"Vol: {spike['volume']} (Base: {spike['baseline_volume']}, Spike: {spike['is_volume_spike']}), "
                             f"AvgP: {spike['avg_price']} (Base: {spike['baseline_avg_price']}, Spike: {spike['is_avg_price_spike']}), "
                            f"MedP: {spike['median_price']} (Base: {spike['baseline_median_price']}, Spike: {spike['is_median_price_spike']})"
                            )
    else:
        logging.info(f"No significant spikes detected for {analysis_target_date} based on current criteria.")


    # --- Cleanup ---
    await db_conn.close()
    end_time_main = datetime.now()
    logging.info(f"--- TCG Market Monitor Finished | Total Time: {end_time_main - start_time_main} ---")

if __name__ == "__main__":
    asyncio.run(run_monitor())