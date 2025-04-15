# analysis.py
import aiosqlite
import logging
from datetime import date, timedelta, datetime
import statistics # For median calculation
from typing import Dict, Optional, List, Tuple
import config
import database # Import database helper functions if needed (or keep DB logic here)

# --- Metric Calculation ---
async def calculate_daily_metrics(conn: aiosqlite.Connection, analysis_date: date):
    """
    Calculates aggregated daily metrics from raw sales for the specified date
    and stores them in the analysis table.
    """
    logging.info(f"Calculating daily metrics for date: {analysis_date}")

    # Query to get all relevant sales for the day, grouped by card
    # Using date() function for robust comparison in SQLite
    query = f"""
        SELECT
            tcgplayer_id,
            COUNT(*) as sales_count,
            SUM(quantity) as daily_volume,
            AVG(purchase_price) as average_price,
            MIN(purchase_price) as min_price,
            MAX(purchase_price) as max_price,
            GROUP_CONCAT(purchase_price) as price_list_str -- For median calculation
        FROM {config.DB_TABLE_SALES}
        WHERE date(order_date) = date(?) -- Compare DATE part
          AND purchase_price > 0 -- Optional filter
        GROUP BY tcgplayer_id
    """

    metrics_to_store = []
    try:
        async with conn.execute(query, (analysis_date.isoformat(),)) as cursor:
            rows = await cursor.fetchall()

        logging.info(f"Found {len(rows)} cards with sales activity to process for {analysis_date}.")

        for row in rows:
            tcgplayer_id, s_count, d_volume, avg_p, min_p, max_p, price_str = row
            median_p = None
            # Calculate median safely
            if price_str:
                try:
                    prices = [float(p) for p in price_str.split(',')]
                    if prices:
                        median_p = statistics.median(prices)
                except (ValueError, TypeError, statistics.StatisticsError) as e:
                    logging.warning(f"Could not calculate median price for {tcgplayer_id} on {analysis_date}: {e}")

            # Ensure defaults for potentially missing values
            metrics_to_store.append((
                tcgplayer_id,
                analysis_date.isoformat(), # Store date as ISO string
                int(d_volume) if d_volume is not None else 0,
                int(s_count) if s_count is not None else 0,
                float(avg_p) if avg_p is not None else 0.0,
                float(median_p) if median_p is not None else None, # Store None if median couldn't be calculated
                float(min_p) if min_p is not None else 0.0,
                float(max_p) if max_p is not None else 0.0
            ))

        if metrics_to_store:
            # Store calculated metrics using INSERT OR REPLACE
            store_sql = f"""
                INSERT OR REPLACE INTO {config.DB_TABLE_ANALYSIS} (
                    tcgplayer_id, analysis_date, daily_volume, sales_count,
                    average_price, median_price, min_price, max_price, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            # Use a transaction for bulk inserts if performance is critical
            await conn.executemany(store_sql, metrics_to_store)
            await conn.commit()
            logging.info(f"Stored/Updated daily metrics for {len(metrics_to_store)} cards for {analysis_date}.")
        else:
            logging.info(f"No sales activity found or processed for {analysis_date}, no metrics stored.")

    except aiosqlite.Error as e:
        logging.error(f"Error calculating or storing daily metrics for {analysis_date}: {e}")
        # Consider raising the exception if analysis should halt on DB errors

# --- Helper to find latest data date ---
async def find_latest_sales_date(conn: aiosqlite.Connection) -> Optional[date]:
    """Finds the most recent date with entries in the sales_records table."""
    # Check analysis table first, maybe it's pre-calculated?
    # query_analysis = f"SELECT MAX(analysis_date) FROM {config.DB_TABLE_ANALYSIS}"
    query_sales = f"SELECT MAX(date(order_date)) FROM {config.DB_TABLE_SALES}"
    try:
        async with conn.execute(query_sales) as cursor:
            result = await cursor.fetchone()
            if result and result[0]:
                latest_date = date.fromisoformat(result[0])
                logging.debug(f"Latest date found in sales data: {latest_date}")
                return latest_date
            else:
                logging.warning("No data found in sales table to determine latest date.")
                return None
    except (aiosqlite.Error, ValueError) as e: # Catch parsing errors too
        logging.error(f"Error finding latest sales date: {e}")
        return None


# --- Main Analysis and Insight Generation ---
async def run_analysis_for_date(conn: aiosqlite.Connection, analysis_date: date) -> Optional[List[Dict[str, any]]]:
    """
    Runs metric calculation and generates insights (like % changes) for a specific date.
    Returns a list of insight dictionaries or None if analysis can't proceed.
    """
    if not analysis_date:
        logging.error("Cannot run analysis: No valid date provided.")
        return None

    logging.info(f"--- Running Analysis & Insight Generation for Date: {analysis_date} ---")

    # 1. Ensure metrics for the target date are calculated and stored
    # This might be redundant if called sequentially, but makes it safe to call independently
    await calculate_daily_metrics(conn, analysis_date)

    # 2. Prepare dates for comparison
    baseline_start_date = analysis_date - timedelta(days=config.ANALYSIS_BASELINE_DAYS)
    previous_date = analysis_date - timedelta(days=1)

    # 3. Query metrics needed for comparison (current day, previous day, baseline average)
    # Query joins metrics for the current date with aggregated baseline data
    # And attempts to join with previous day's data
    query = f"""
    WITH RelevantMetrics AS (
        -- Select data for the period covering baseline up to analysis date
        SELECT
            tcgplayer_id, analysis_date, daily_volume, sales_count, average_price, median_price
        FROM {config.DB_TABLE_ANALYSIS}
        WHERE analysis_date >= ? AND analysis_date <= ? -- Window: baseline start to analysis day
    ),
    CurrentData AS (
        SELECT * FROM RelevantMetrics WHERE analysis_date = ?
    ),
    PreviousData AS (
        SELECT * FROM RelevantMetrics WHERE analysis_date = ?
    ),
    BaselineData AS (
        -- Calculate baseline average *excluding* the analysis_date
        SELECT
            tcgplayer_id,
            AVG(daily_volume) as avg_baseline_volume,
            AVG(average_price) as avg_baseline_price,
            AVG(median_price) as avg_baseline_median_price -- Careful with AVG of MEDIAN
        FROM RelevantMetrics
        WHERE analysis_date < ? -- Baseline = days BEFORE analysis_date
          AND analysis_date >= ? -- Only consider days within baseline window
        GROUP BY tcgplayer_id
    )
    -- Join current day's metrics with previous day's and baseline averages
    SELECT
        c.tcgplayer_id,
        c.analysis_date,
        c.daily_volume AS current_volume,
        c.average_price AS current_avg_price,
        c.median_price AS current_median_price,
        c.sales_count AS current_sales_count,
        -- Previous day's data (use COALESCE for cards with no sales yesterday)
        COALESCE(p.daily_volume, 0) AS prev_volume,
        COALESCE(p.average_price, 0) AS prev_avg_price,
        COALESCE(p.median_price, 0) AS prev_median_price, -- Median can be tricky with 0
        -- Baseline averages (use COALESCE for cards with no baseline history)
        COALESCE(b.avg_baseline_volume, 0) as baseline_volume,
        COALESCE(b.avg_baseline_price, 0) as baseline_avg_price,
        COALESCE(b.avg_baseline_median_price, 0) as baseline_median_price
    FROM CurrentData c
    LEFT JOIN PreviousData p ON c.tcgplayer_id = p.tcgplayer_id
    LEFT JOIN BaselineData b ON c.tcgplayer_id = b.tcgplayer_id;
    """

    insights = []
    try:
        # Execute the query with parameters for the date window
        async with conn.execute(query, (
            baseline_start_date.isoformat(), # Start date for RelevantMetrics window
            analysis_date.isoformat(),     # End date for RelevantMetrics window
            analysis_date.isoformat(),     # Filter for CurrentData
            previous_date.isoformat(),     # Filter for PreviousData
            analysis_date.isoformat(),     # Upper bound (exclusive) for BaselineData date filter
            baseline_start_date.isoformat() # Lower bound (inclusive) for BaselineData date filter
            )) as cursor:
            rows = await cursor.fetchall()

        logging.info(f"Comparing metrics for {len(rows)} cards for {analysis_date} against previous day and {config.ANALYSIS_BASELINE_DAYS}-day baseline.")

        for row in rows:
            (tcg_id, an_date_iso, vol, avg_p, med_p, s_count,
             prev_vol, prev_avg_p, prev_med_p,
             base_vol, base_avg_p, base_med_p) = row

            # Safely cast median prices (can be None/0 from COALESCE)
            current_median = float(med_p) if med_p is not None else None
            prev_median = float(prev_med_p) if prev_med_p is not None else None
            base_median = float(base_med_p) if base_med_p is not None else None

            # --- Calculate Percentage Changes (handle division by zero) ---
            def calculate_pct_change(current, previous):
                if previous and previous != 0:
                    return ((current - previous) / previous) * 100
                elif current and current != 0:
                    return float('inf') # Change from zero to non-zero
                else:
                    return 0.0 # No change (0 to 0) or invalid previous

            vol_change_pct = calculate_pct_change(vol, prev_vol)
            avg_p_change_pct = calculate_pct_change(avg_p, prev_avg_p)
            med_p_change_pct = calculate_pct_change(current_median, prev_median) if current_median is not None and prev_median is not None else None

            # --- Apply Spike Logic (compare against baseline) ---
            is_vol_spike = (vol >= config.ANALYSIS_VOLUME_SPIKE_MIN_ABSOLUTE and
                           ((base_vol > 0.5 and vol > base_vol * config.ANALYSIS_VOLUME_SPIKE_MULTIPLIER)
                            or (base_vol <= 0.5 and vol > 0))) # Spike if baseline is near zero and current is > min_absolute

            is_avg_p_spike = (avg_p and avg_p > 0.1 and base_avg_p and base_avg_p > 0.1 and
                              avg_p > base_avg_p * config.ANALYSIS_PRICE_SPIKE_MULTIPLIER)

            is_med_p_spike = (current_median and current_median > 0.1 and base_median and base_median > 0.1 and
                              current_median > base_median * config.ANALYSIS_PRICE_MEDIAN_SPIKE_MULTIPLIER)

            # --- Format and Store Insight ---
            insights.append({
                "tcgplayer_id": tcg_id,
                "date": an_date_iso,
                "volume": int(vol),
                "avg_price": round(float(avg_p), 2) if avg_p is not None else 0.0,
                "median_price": round(current_median, 2) if current_median is not None else None,
                "sales_count": int(s_count),
                # Format percentage changes for display
                "vol_change_pct": round(vol_change_pct, 1) if vol_change_pct != float('inf') else 'inf',
                "avg_p_change_pct": round(avg_p_change_pct, 1) if avg_p_change_pct != float('inf') else 'inf',
                "med_p_change_pct": round(med_p_change_pct, 1) if med_p_change_pct is not None and med_p_change_pct != float('inf') else ('inf' if med_p_change_pct == float('inf') else None),
                # Baseline and spike flags
                "baseline_volume": round(float(base_vol), 1) if base_vol is not None else 0.0,
                "baseline_avg_price": round(float(base_avg_p), 2) if base_avg_p is not None else 0.0,
                "baseline_median_price": round(float(base_median), 2) if base_median is not None else None,
                "is_volume_spike": is_vol_spike,
                "is_avg_price_spike": is_avg_p_spike,
                "is_median_price_spike": is_med_p_spike,
            })

    except aiosqlite.Error as e:
        logging.error(f"Error during analysis comparison query for {analysis_date}: {e}")
        return None # Return None to indicate analysis failed

    except Exception as e: # Catch potential errors in calculation/formatting
        logging.error(f"Unexpected error during insight generation for {analysis_date}: {e}", exc_info=True)
        return None


    logging.info(f"Finished analysis comparison for {analysis_date}. Generated {len(insights)} insight records.")
    return insights