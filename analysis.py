# analysis.py
import aiosqlite
import logging
from datetime import date, timedelta, datetime
import statistics # For median calculation
from typing import Dict, Optional, List, Tuple
import config
import database # Import database helper functions

async def calculate_daily_metrics(conn: aiosqlite.Connection, analysis_date: date):
    """
    Calculates aggregated daily metrics (volume, avg/median price) from the
    raw sales data for the specified date and stores them.
    """
    logging.info(f"Calculating daily metrics for date: {analysis_date}")
    start_date = analysis_date.isoformat() + " 00:00:00"
    end_date = analysis_date.isoformat() + " 23:59:59"

    # Query to get all relevant sales for the day, grouped by card
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
        WHERE order_date >= ? AND order_date <= ?
          AND purchase_price > 0 -- Optional: Filter out $0 sales if necessary
          --AND condition = 'Near Mint' -- Optional: Filter for specific conditions
        GROUP BY tcgplayer_id
    """

    metrics_to_store = []
    try:
        async with conn.execute(query, (start_date, end_date)) as cursor:
            rows = await cursor.fetchall()

        logging.info(f"Found {len(rows)} cards with sales activity on {analysis_date}.")

        for row in rows:
            tcgplayer_id, s_count, d_volume, avg_p, min_p, max_p, price_str = row
            median_p = None
            if price_str:
                try:
                    prices = [float(p) for p in price_str.split(',')]
                    if prices:
                        median_p = statistics.median(prices)
                except Exception as e:
                    logging.warning(f"Could not calculate median price for {tcgplayer_id} on {analysis_date}: {e}")

            metrics_to_store.append((
                tcgplayer_id,
                analysis_date.isoformat(), # Store date as ISO string
                d_volume if d_volume else 0,
                s_count if s_count else 0,
                avg_p if avg_p else 0.0,
                median_p if median_p else None, # Store None if median couldn't be calculated
                min_p if min_p else 0.0,
                max_p if max_p else 0.0
            ))

        if metrics_to_store:
            # Store calculated metrics using INSERT OR REPLACE (or UPDATE)
            store_sql = f"""
                INSERT OR REPLACE INTO {config.DB_TABLE_ANALYSIS} (
                    tcgplayer_id, analysis_date, daily_volume, sales_count,
                    average_price, median_price, min_price, max_price, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            await conn.executemany(store_sql, metrics_to_store)
            await conn.commit()
            logging.info(f"Stored/Updated daily metrics for {len(metrics_to_store)} cards for {analysis_date}.")
        else:
            logging.info(f"No sales metrics to store for {analysis_date}.")

    except aiosqlite.Error as e:
        logging.error(f"Error calculating or storing daily metrics for {analysis_date}: {e}")

async def detect_spikes(conn: aiosqlite.Connection, check_date: date) -> List[Dict[str, any]]:
    """
    Compares metrics for `check_date` against baseline averages to detect spikes.
    Returns a list of dictionaries, each describing a detected spike.
    """
    logging.info(f"Starting spike detection for date: {check_date}")
    spikes_found = []
    baseline_start_date = check_date - timedelta(days=config.ANALYSIS_BASELINE_DAYS)

    # Query to get today's metrics and the baseline averages in one go
    query = f"""
    WITH TodayMetrics AS (
        SELECT * FROM {config.DB_TABLE_ANALYSIS}
        WHERE analysis_date = ?
    ),
    BaselineMetrics AS (
        SELECT
            tcgplayer_id,
            AVG(daily_volume) as avg_baseline_volume,
            AVG(average_price) as avg_baseline_price,
            AVG(median_price) as avg_baseline_median_price -- Use AVG of daily MEDIANs as baseline median
        FROM {config.DB_TABLE_ANALYSIS}
        WHERE analysis_date >= ? AND analysis_date < ? -- Baseline up to *day before* check_date
        GROUP BY tcgplayer_id
    )
    SELECT
        t.tcgplayer_id,
        t.analysis_date,
        t.daily_volume,
        t.sales_count,
        t.average_price,
        t.median_price,
        COALESCE(b.avg_baseline_volume, 0) as baseline_volume,
        COALESCE(b.avg_baseline_price, 0) as baseline_avg_price,
        COALESCE(b.avg_baseline_median_price, 0) as baseline_median_price
    FROM TodayMetrics t
    LEFT JOIN BaselineMetrics b ON t.tcgplayer_id = b.tcgplayer_id;
    """

    try:
        async with conn.execute(query, (check_date.isoformat(), baseline_start_date.isoformat(), check_date.isoformat())) as cursor:
            rows = await cursor.fetchall()

        logging.info(f"Comparing metrics for {len(rows)} cards against baseline for {check_date}.")

        for row in rows:
            (tcg_id, an_date, volume, s_count, avg_p, med_p,
             base_vol, base_avg_p, base_med_p) = row

            # Check for Volume Spike
            volume_spike = False
            if base_vol > 0.5: # Require some baseline activity
                 if (volume > config.ANALYSIS_VOLUME_SPIKE_MIN_ABSOLUTE and
                     volume > base_vol * config.ANALYSIS_VOLUME_SPIKE_MULTIPLIER):
                     volume_spike = True
            elif volume > config.ANALYSIS_VOLUME_SPIKE_MIN_ABSOLUTE:
                 # Spike compared to zero baseline (if absolute minimum met)
                 volume_spike = True

            # Check for Average Price Spike
            price_spike_avg = False
            if base_avg_p > 0.1: # Require some baseline price
                if avg_p > base_avg_p * config.ANALYSIS_PRICE_SPIKE_MULTIPLIER:
                    price_spike_avg = True

            # Check for Median Price Spike (if available)
            price_spike_med = False
            if med_p is not None and base_med_p > 0.1:
                 if med_p > base_med_p * config.ANALYSIS_PRICE_MEDIAN_SPIKE_MULTIPLIER:
                     price_spike_med = True

            # Report Spike if any criteria met
            if volume_spike or price_spike_avg or price_spike_med:
                spike_details = {
                    "tcgplayer_id": tcg_id,
                    "date": an_date,
                    "volume": volume,
                    "baseline_volume": round(base_vol, 1),
                    "is_volume_spike": volume_spike,
                    "avg_price": round(avg_p, 2) if avg_p else 0.0,
                    "baseline_avg_price": round(base_avg_p, 2),
                    "is_avg_price_spike": price_spike_avg,
                    "median_price": round(med_p, 2) if med_p else None,
                    "baseline_median_price": round(base_med_p, 2) if base_med_p else None,
                    "is_median_price_spike": price_spike_med,
                    "sales_count": s_count,
                }
                spikes_found.append(spike_details)
                logging.info(f"Spike Detected: {spike_details}") # Log detected spike details

    except aiosqlite.Error as e:
        logging.error(f"Error during spike detection query for {check_date}: {e}")

    logging.info(f"Finished spike detection for {check_date}. Found {len(spikes_found)} spikes.")
    return spikes_found