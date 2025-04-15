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

# Configure logging (consider moving detailed config to a helper function or module)
logging.basicConfig(
    level=logging.INFO, # Adjust level (DEBUG, INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Helper: Fetch Target IDs from Supabase ---
async def get_target_ids(supabase: Client) -> List[str]:
    """Fetches tcgplayer_ids from Supabase, filtering by TARGET_SETS in config,
       using pagination to avoid timeouts."""
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
            # Base query builder for each page
            query_builder = supabase.table(config.SUPABASE_TABLE_CARDS).select(select_query, count='exact').limit(ID_FETCH_PAGE_SIZE).offset(current_offset)

            # Apply set filter if specified
            if config.TARGET_SETS:
                query_builder = query_builder.in_(config.SUPABASE_COLUMN_SET, config.TARGET_SETS)

            # Execute the paginated query
            response = await loop.run_in_executor(None, query_builder.execute)

            # Check response structure
            if not hasattr(response, 'data'):
                logging.error(f"Supabase response missing 'data' attribute at offset {current_offset}. Response: {response}")
                break # Stop if response structure is unexpected

            # Process response for the current page
            if response.data:
                ids_on_page = [str(item[config.SUPABASE_COLUMN_ID]) for item in response.data
                               if config.SUPABASE_COLUMN_ID in item and item[config.SUPABASE_COLUMN_ID]]
                valid_ids_on_page = {id_ for id_ in ids_on_page if id_.strip()}

                if not valid_ids_on_page:
                    logging.debug(f"Data array present but no valid IDs found at offset {current_offset}. Ending fetch.")
                    break

                all_valid_ids.update(valid_ids_on_page)
                logging.info(f"Fetched {len(valid_ids_on_page)} IDs on this page. Total unique IDs so far: {len(all_valid_ids)}")

                # Check if this was the last page based on fetched count vs limit
                if len(response.data) < ID_FETCH_PAGE_SIZE:
                    logging.debug("Fetched fewer IDs than page size, likely the last page.")
                    break
                else:
                    current_offset += ID_FETCH_PAGE_SIZE
                    await asyncio.sleep(1.1) # Small delay between page fetches
            else:
                 # Got a valid response structure, but data array is empty``
                 logging.info(f"Received empty data array at offset {current_offset}. Ending fetch.")
                 break # No more data

    # --- Error Handling ---
    except Exception as e:
        # Handle Postgrest API errors more gracefully
        if "APIError" in str(type(e)) and hasattr(e, 'json') and callable(e.json):
             try:
                 error_details = e.json()
                 logging.error(f"Supabase API Error fetching target IDs: {error_details}", exc_info=False)
             except Exception as json_e:
                 logging.error(f"Supabase API Error (Could not decode JSON): {e}", exc_info=False)
        elif "APIError" in str(type(e)): # If json() fails or doesn't exist
             logging.error(f"Supabase API Error: {e}", exc_info=False)
        else:
             # Log other exceptions with traceback
             logging.error(f"Unexpected Error fetching target IDs: {e}", exc_info=True)
        return [] # Return empty list on error

    final_id_list = sorted(list(all_valid_ids))
    logging.info(f"Finished fetching target IDs. Total unique IDs retrieved: {len(final_id_list)}")
    return final_id_list


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
            # Consider if Supabase is essential for this run
            # return # Exit if Supabase is required and failed
    else:
        logging.warning("Supabase URL or Key not configured. Cannot fetch target IDs.")
        # Optionally proceed if target_ids can be obtained otherwise
        return # Exit if Supabase needed for target IDs

    # --- Initialize Database ---
    db_conn = await database.get_db_connection()
    await database.setup_schema(db_conn)

    # --- Load Proxies ---
    loaded_proxies = proxies.load_proxies()
    proxy_list = loaded_proxies if config.USE_PROXIES and loaded_proxies else [None] # Use list with None for direct/fallback
    logging.info(f"Proxy setup complete. Using {len(loaded_proxies) if config.USE_PROXIES else 0} proxies (or direct connection).")


    # --- Fetch Target IDs ---
    target_ids = await get_target_ids(supabase_client) if supabase_client else []
    if not target_ids:
        logging.warning("No target IDs to process. Exiting fetch phase.")
        # Continue to analysis phase? Maybe data already exists.
        # Set fetch_completed_successfully = False here if needed
    else:
        logging.info(f"Successfully obtained {len(target_ids)} target IDs.")

    # --- Fetching Loop (Sticky Proxy Logic) ---
    current_proxy_index = 0
    remaining_ids: Set[str] = set(target_ids) # Start with fetched IDs
    id_retry_counts: Dict[str, int] = {item_id: 0 for item_id in target_ids}
    failed_ids_permanently: Set[str] = set()
    successful_fetch_ids: Set[str] = set()
    store_tasks: List[asyncio.Task] = [] # List to hold storage tasks

    if not remaining_ids:
         logging.info("Skipping fetching phase as there are no target IDs.")
    else:
        logging.info(f"Starting fetch process for {len(remaining_ids)} target IDs...")
        # Reusable AIOHTTP session
        async with aiohttp.ClientSession() as session:
            while remaining_ids:
                if current_proxy_index >= len(proxy_list):
                     logging.error(f"Exhausted all {len(proxy_list)} proxy/direct options. {len(remaining_ids)} IDs could not be fetched.")
                     failed_ids_permanently.update(remaining_ids) # Mark rest as failed
                     remaining_ids.clear() # Stop the loop
                     break

                # Determine proxy for this batch
                current_proxy_config = proxy_list[current_proxy_index]
                proxy_host_info = "Direct Connection" if current_proxy_config is None else current_proxy_config.get('host', 'N/A')
                proxy_log_msg = f"Proxy #{current_proxy_index} ({proxy_host_info})"

                logging.info(f"--- Starting Fetch Batch | IDs Remaining: {len(remaining_ids)} | Using: {proxy_log_msg} ---")

                # Create batch of tasks
                tasks_in_batch = []
                ids_in_batch = list(remaining_ids)[:config.MAX_CONCURRENT_REQUESTS]

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
                ids_succeeded_this_batch = set()
                ids_failed_non_proxy_this_batch = set()

                for item_id, result in zip(ids_in_batch, results):
                    if isinstance(result, ProxyError):
                         proxy_failed_this_batch = True
                         logging.warning(f"ID {item_id}: ProxyError ({result}). Will retry with next proxy.")
                         # Note: Retry count isn't incremented here, as it's a proxy issue
                    elif isinstance(result, Exception):
                        logging.error(f"ID {item_id}: Failed fetch with non-proxy error ({type(result).__name__}: {result}).")
                        id_retry_counts[item_id] += 1
                        if id_retry_counts[item_id] >= config.MAX_RETRIES_PER_ID:
                            logging.error(f"ID {item_id}: Max retries ({config.MAX_RETRIES_PER_ID}) reached. Marking failed permanently.")
                            failed_ids_permanently.add(item_id)
                            ids_failed_non_proxy_this_batch.add(item_id) # Remove from remaining
                        else:
                            logging.info(f"ID {item_id}: Will retry non-proxy error (Attempt {id_retry_counts[item_id]}/{config.MAX_RETRIES_PER_ID}).")
                            # ID remains in remaining_ids
                    elif isinstance(result, list): # Success returns a list of sales dicts
                        logging.debug(f"ID {item_id}: Successfully fetched {len(result)} sales.")
                        ids_succeeded_this_batch.add(item_id)
                        successful_fetch_ids.add(item_id)
                        # Schedule storage task (check if there's data)
                        if result: # Only store if the list is not empty
                           store_tasks.append(asyncio.create_task(
                               database.store_sales_batch(db_conn, item_id, result),
                               name=f"Store-{item_id}"
                           ))
                        # else: item fetched successfully but returned 0 sales
                    else: # Unexpected success result type
                        logging.error(f"ID {item_id}: Received unexpected result type: {type(result)}. Marking failed permanently.")
                        failed_ids_permanently.add(item_id)
                        ids_failed_non_proxy_this_batch.add(item_id) # Remove from remaining


                # --- Update State After Batch ---
                remaining_ids -= ids_succeeded_this_batch
                remaining_ids -= ids_failed_non_proxy_this_batch


                # --- Decide Next Action ---
                if proxy_failed_this_batch: # If ANY task in batch hit ProxyError
                    logging.warning(f"{proxy_log_msg} flagged with ProxyError. Switching proxy for next batch.")
                    current_proxy_index += 1 # Move to next proxy
                    # Resetting retry counts could be considered here if needed
                elif not remaining_ids:
                    logging.info("All processable IDs completed in this fetching run.") # Exit loop condition
                    break # Explicit break for clarity
                else:
                     # Proxy seems okay, continue using it for the next batch
                     logging.debug(f"Proxy {proxy_log_msg} was stable this batch. Continuing.")

                # --- Add delay between batches ---
                await asyncio.sleep(0.5) # 100 millisecond delay


        logging.info("--- Fetching phase complete ---")
        logging.info(f"Successfully completed fetch attempts for {len(successful_fetch_ids)} IDs.")
        if failed_ids_permanently:
            logging.warning(f"{len(failed_ids_permanently)} IDs failed permanently after retries.")
            # Log first few failed IDs for debugging
            failed_sample = sorted(list(failed_ids_permanently))[:10]
            logging.warning(f"Sample permanently failed IDs: {failed_sample}{'...' if len(failed_ids_permanently) > 10 else ''}")


    # --- Wait for and log storage results ---
    if store_tasks:
        logging.info(f"Waiting for {len(store_tasks)} storage tasks to complete...")
        storage_results = await asyncio.gather(*store_tasks, return_exceptions=True)
        total_inserted_count = 0
        storage_errors = 0
        successful_stores = 0
        for res in storage_results:
            if isinstance(res, Exception):
                 # Correlate error back to task name if needed (more complex logging)
                 logging.error(f"A storage task failed: {res}", exc_info=False) # Set exc_info=True for stack trace
                 storage_errors += 1
            elif isinstance(res, int): # store_sales_batch returns the count of inserted rows
                 total_inserted_count += res
                 if res >= 0: successful_stores +=1 # Count task success even if 0 inserted
            else:
                 logging.warning(f"Unexpected result from storage task: {type(res)}")

        logging.info(f"Storage phase completed.")
        logging.info(f"Number of storage tasks processed: {len(store_tasks)}")
        logging.info(f"Total NEW sales records inserted into DB: {total_inserted_count}")
        if storage_errors > 0:
            logging.error(f"{storage_errors} storage tasks encountered errors.")
    else:
         logging.info("No storage tasks were scheduled (either no successful fetches or fetched data was empty).")


     # --- Analysis Phase ---
    logging.info(f"--- Starting Analysis Phase ---")

    # 1. Find the date range covered by NEWLY INSERTED data (if possible)
    # We need the min/max date from the data inserted in THIS RUN.
    # Getting this accurately after the fact without complex tracking is tricky.
    # ALTERNATIVE: Find overall Min/Max dates in DB and process range.
    async def find_sales_date_range(conn: aiosqlite.Connection) -> Tuple[Optional[date], Optional[date]]:
        query = f"SELECT MIN(date(order_date)), MAX(date(order_date)) FROM {config.DB_TABLE_SALES}"
        try:
            async with conn.execute(query) as cursor:
                result = await cursor.fetchone()
                if result and result[0] and result[1]:
                    min_d = date.fromisoformat(result[0])
                    max_d = date.fromisoformat(result[1])
                    logging.debug(f"Sales data date range found in DB: {min_d} to {max_d}")
                    return min_d, max_d
                else:
                    logging.warning("No date range found in sales table.")
                    return None, None
        except Exception as e:
            logging.error(f"Error finding sales date range: {e}")
            return None, None

    min_sales_date, max_sales_date = await find_sales_date_range(db_conn)

    if min_sales_date and max_sales_date:
        logging.info(f"Processing analysis for date range: {min_sales_date} to {max_sales_date}")

        # Determine dates to calculate metrics for.
        # Strategy: Ensure metrics are calculated for all days from the earliest
        # relevant baseline date up to the latest sales date found.
        analysis_latest_date = max_sales_date
        analysis_earliest_needed = min(min_sales_date, analysis_latest_date - timedelta(days=config.ANALYSIS_BASELINE_DAYS + 2)) # Go back baseline+buffer days

        current_calc_date = analysis_earliest_needed
        logging.info(f"Ensuring daily metrics are calculated from {analysis_earliest_needed} to {analysis_latest_date}...")

        # Loop to calculate metrics for any missing days in the relevant range
        while current_calc_date <= analysis_latest_date:
            # Optional check: Only calculate if metrics for this date don't exist yet?
            # This adds DB query overhead but prevents recalculation.
            # needs_calc = await check_if_metrics_exist(db_conn, current_calc_date) # Need to implement check_if_metrics_exist
            # if needs_calc:
            #    await analysis.calculate_daily_metrics(db_conn, current_calc_date)

            # Simpler: Always recalculate/update for the necessary range
            await analysis.calculate_daily_metrics(db_conn, current_calc_date)
            current_calc_date += timedelta(days=1)

        logging.info("Finished calculating/updating daily metrics for the relevant date range.")

        # Now run the insight generation using the LATEST date for comparison
        logging.info(f"Generating insights comparing {analysis_latest_date} to previous day and baseline...")
        all_insights = await analysis.run_analysis_for_date(db_conn, analysis_latest_date) # Uses the calculation done above

        # --- Reporting Phase ---
        if all_insights:
            logging.warning(f"--- Generated {len(all_insights)} Insights for {analysis_latest_date} ---") # Report for the latest date

            # Helper function for safe formatting of percentages
            def format_pct(value):
                # ... (same formatting helper as before) ...
                if isinstance(value, (int, float)) and value != float('inf'): return f"{value:+7.1f}%"
                elif value == 'inf': return " (+inf%)"
                else: return " ( N/A )"

            # Sort by different metrics (unchanged from previous correction)
            top_volume_movers = sorted(
                [i for i in all_insights if isinstance(i.get('vol_change_pct'), (int, float)) and i.get('volume', 0) >= 3],
                key=lambda x: x.get('vol_change_pct', -float('inf')), reverse=True
            )[:15]
            top_price_movers = sorted(
                [i for i in all_insights if isinstance(i.get('avg_p_change_pct'), (int, float)) and i.get('avg_price', 0) >= 1.00],
                key=lambda x: x.get('avg_p_change_pct', -float('inf')), reverse=True
            )[:15]
            top_abs_volume = sorted(
                [i for i in all_insights if i.get('volume', 0) >= 5],
                key=lambda x: x.get('volume', 0), reverse=True
            )[:15]

            logging.warning("--- Top Volume Movers (% Change vs Prev Day | Vol >= 3) ---")
            if top_volume_movers:
                for item in top_volume_movers:
                     vol_pct_str = format_pct(item.get('vol_change_pct'))
                     logging.warning(f"  ID: {item['tcgplayer_id']:<10} Vol: {item['volume']:<4} {vol_pct_str} AvgP: ${item['avg_price']:.2f}")
            else:
                 logging.warning("  (No cards met Volume % Change criteria for latest day)")

            logging.warning("--- Top Avg Price Movers (% Change vs Prev Day | AvgP >= $1.00) ---")
            if top_price_movers:
                for item in top_price_movers:
                     avg_p_pct_str = format_pct(item.get('avg_p_change_pct'))
                     logging.warning(f"  ID: {item['tcgplayer_id']:<10} AvgP: ${item['avg_price']:<7.2f} {avg_p_pct_str} Vol: {item['volume']}")
            else:
                logging.warning("  (No cards met Avg Price % Change criteria for latest day)")

            logging.warning("--- Highest Volume Cards (Latest Day | Vol >= 5) ---")
            if top_abs_volume:
                 for item in top_abs_volume:
                      change_str = format_pct(item.get('avg_p_change_pct')) # Use correct helper
                      logging.warning(f"  ID: {item['tcgplayer_id']:<10} Vol: {item['volume']:<4} AvgP: ${item['avg_price']:.2f} Change:{change_str}")
            else:
                logging.warning("  (No cards met Highest Volume criteria for latest day)")
        else:
             logging.info(f"Analysis for {analysis_latest_date} did not generate insights (potentially no comparable data).")

    else:
        logging.warning("No sales data found in the database to perform analysis.")


    # --- Cleanup ---
    await db_conn.close()
    logging.info("Database connection closed.")
    end_time_main = datetime.now()
    logging.info(f"--- TCG Market Monitor Finished | Total Time: {end_time_main - start_time_main} ---")


if __name__ == "__main__":
    asyncio.run(run_monitor())