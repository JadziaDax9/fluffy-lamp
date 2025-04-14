# database.py
import aiosqlite
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import config # Import project config

# --- Connection Handling (Basic for SQLite) ---
async def get_db_connection() -> aiosqlite.Connection:
    """Establishes and returns a connection to the SQLite database."""
    # For SQLite, the connection is file-based.
    # For Postgres, this would involve connection pooling (e.g., asyncpg).
    conn = await aiosqlite.connect(config.SQLITE_DB_NAME)
    await conn.execute("PRAGMA journal_mode=WAL;") # Improve write concurrency for SQLite
    await conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# --- Schema Setup ---
async def setup_schema(conn: aiosqlite.Connection):
    """Creates necessary tables if they don't exist."""
    try:
        # Structured Sales Records Table
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_TABLE_SALES} (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- Unique ID for the sale
                tcgplayer_id TEXT NOT NULL,
                order_date DATETIME NOT NULL,
                purchase_price REAL NOT NULL,
                shipping_price REAL,
                quantity INTEGER DEFAULT 1,
                condition TEXT,
                variant TEXT,
                language TEXT,
                listing_type TEXT,
                fetch_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                -- Constraint to prevent duplicate sales entries (adjust if needed)
                UNIQUE(tcgplayer_id, order_date, purchase_price, quantity, condition, variant, listing_type)
            )
        """)
        # Indexes for efficient querying
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sales_tcgplayer_id ON {config.DB_TABLE_SALES}(tcgplayer_id);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sales_order_date ON {config.DB_TABLE_SALES}(order_date);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sales_id_date ON {config.DB_TABLE_SALES}(tcgplayer_id, order_date);")

        # Table to store daily aggregated metrics (for analysis)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_TABLE_ANALYSIS} (
                metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tcgplayer_id TEXT NOT NULL,
                analysis_date DATE NOT NULL, -- Date for which metrics are calculated
                daily_volume INTEGER,        -- Total quantity sold
                sales_count INTEGER,         -- Number of distinct sales transactions
                average_price REAL,
                median_price REAL,
                min_price REAL,
                max_price REAL,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tcgplayer_id, analysis_date) -- Ensure only one entry per card per day
            )
        """)
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_metrics_id_date ON {config.DB_TABLE_ANALYSIS}(tcgplayer_id, analysis_date);")

        await conn.commit()
        logging.info(f"Database schema setup complete for tables: {config.DB_TABLE_SALES}, {config.DB_TABLE_ANALYSIS}")
    except aiosqlite.Error as e:
        logging.error(f"Error setting up database schema: {e}")
        raise

# --- Storing Sales Data ---
def parse_datetime(date_string: str) -> Optional[datetime]:
    """Safely parse ISO 8601 format datetime strings, handling potential milliseconds and timezone."""
    if not date_string:
        return None
    try:
        # Try parsing with microseconds first
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except ValueError:
        try:
            # Try parsing without microseconds if the first attempt failed
             # Handle formats like "2025-04-14T18:32:14+00:00" sometimes returned
            if '.' not in date_string and '+' in date_string:
                 # This format might require specific parsing or manipulation depending on exact structure
                 # Let's assume standard ISO if microseconds are simply missing
                 return datetime.fromisoformat(date_string)
            else:
                logging.warning(f"Could not parse datetime string: {date_string}")
                return None
        except ValueError:
            logging.warning(f"Could not parse datetime string after retry: {date_string}")
            return None


async def store_sales_batch(conn: aiosqlite.Connection, tcgplayer_id: str, sales_data: List[Dict[str, Any]]):
    """Stores a batch of structured sales data, avoiding duplicates."""
    insert_sql = f"""
        INSERT OR IGNORE INTO {config.DB_TABLE_SALES} (
            tcgplayer_id, order_date, purchase_price, shipping_price, quantity,
            condition, variant, language, listing_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    records_to_insert = []
    parse_errors = 0
    valid_sales_found = 0

    for sale in sales_data:
        order_dt = parse_datetime(sale.get("orderDate"))
        if not order_dt:
            parse_errors += 1
            continue # Skip sales with unparseable dates

        try:
            price = float(sale.get("purchasePrice", 0.0))
            shipping = float(sale.get("shippingPrice", 0.0))
            quantity = int(sale.get("quantity", 1))
            valid_sales_found += 1
            records_to_insert.append((
                tcgplayer_id,
                order_dt.isoformat(), # Store as ISO string for SQLite
                price,
                shipping,
                quantity,
                sale.get("condition"),
                sale.get("variant"),
                sale.get("language"),
                sale.get("listingType"),
            ))
        except (ValueError, TypeError) as e:
            logging.warning(f"Skipping sale record due to type error for ID {tcgplayer_id}: {e}. Data: {sale}")
            parse_errors +=1


    if parse_errors > 0:
         logging.warning(f"ID {tcgplayer_id}: Encountered {parse_errors} errors parsing dates or numeric fields in sales batch.")

    if not records_to_insert:
        logging.debug(f"ID {tcgplayer_id}: No valid, new sales records found in this batch to insert.")
        return 0 # No rows inserted

    try:
        async with conn.cursor() as cursor:
            await cursor.executemany(insert_sql, records_to_insert)
            await conn.commit()
            inserted_count = cursor.rowcount
            logging.debug(f"ID {tcgplayer_id}: Attempted to insert {len(records_to_insert)} records, successfully inserted {inserted_count} new sales.")
            return inserted_count
    except aiosqlite.Error as e:
        logging.error(f"Error storing sales batch for ID {tcgplayer_id}: {e}")
        # Consider rolling back if in a transaction elsewhere
        return 0 # Indicate failure

# --- Querying (Example - Get Max Order Date) ---
async def get_latest_sale_timestamp(conn: aiosqlite.Connection, tcgplayer_id: str) -> Optional[datetime]:
    """Gets the timestamp of the most recent sale stored for a specific card."""
    # THIS IS USEFUL FOR INCREMENTAL FETCHING (Future improvement)
    # For now, we fetch all, so this isn't strictly needed, but good to have
    sql = f"SELECT MAX(order_date) FROM {config.DB_TABLE_SALES} WHERE tcgplayer_id = ?"
    try:
        async with conn.execute(sql, (tcgplayer_id,)) as cursor:
            result = await cursor.fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None
    except aiosqlite.Error as e:
        logging.error(f"Error getting latest timestamp for ID {tcgplayer_id}: {e}")
        return None

# --- Add querying functions for analysis later in analysis.py ---