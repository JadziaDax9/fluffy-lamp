# fetcher.py
import asyncio
import aiohttp
import logging
import json
from typing import Optional, Tuple, List, Dict, Any
import config # Import project config
from proxies import ProxyDict # Import type alias if defined in proxies.py

# --- Custom Exception for Proxy Failures ---
class ProxyError(Exception):
    """Custom exception for proxy-related failures (Timeout, Connection, 403, 407)."""
    pass

# --- Function to format proxy URL (Helper - unchanged) ---
def format_proxy_url(proxy_dict: Optional[ProxyDict]) -> Optional[str]:
    """Formats a proxy dictionary into a URL string for aiohttp."""
    # ... (implementation is the same as before)
    if not proxy_dict:
        return None
    protocol = proxy_dict.get('protocol', 'https')
    host = proxy_dict.get('host')
    port = proxy_dict.get('port')
    username = proxy_dict.get('username') # Pre-encoded
    password = proxy_dict.get('password') # Pre-encoded
    if not host or not port:
        logging.warning(f"Invalid proxy dict data: {proxy_dict}, cannot format URL.")
        return None
    user_pass = ""
    if username and password: user_pass = f"{username}:{password}@"
    elif username: user_pass = f"{username}@"
    return f"{protocol}://{user_pass}{host}:{port}"


# --- Core Fetching Logic ---
async def fetch_sales_for_id(
    session: aiohttp.ClientSession,
    proxy_dict: Optional[ProxyDict],
    item_id: str,
) -> List[Dict[str, Any]]:
    """
    Fetches all recent sales pages for a TCGPlayer ID using the provided proxy.
    Returns a list of individual sale dictionaries on success.
    Raises ProxyError for connection/timeout/403/407 issues.
    Raises other exceptions for target server errors or data format errors.
    """
    target_url_template = config.TCGPLAYER_API_URL_TEMPLATE
    base_payload = {
        "conditions": [], "languages": [1], "variants": [],
        "listingType": "All", "limit": config.TCGPLAYER_PAGE_LIMIT, "offset": 0
    }
    headers = { # Example headers
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.tcgplayer.com",
        "referer": "https://www.tcgplayer.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36" # Example UA
    }
    querystring = {"mpfev": "3302"} # Example query string

    all_sales_data: List[Dict[str, Any]] = []
    current_offset = 0
    page_num = 1

    target_url = target_url_template.format(item_id)
    proxy_url = format_proxy_url(proxy_dict)
    proxy_log_msg = f"Proxy: {proxy_url}" if proxy_url else "Proxy: Direct"
    logging.debug(f"ID {item_id}: Starting fetch ({proxy_log_msg})")

    while True:
        current_payload = base_payload.copy()
        current_payload["offset"] = current_offset

        try:
            logging.debug(f"ID {item_id} Page {page_num} Offset {current_offset}: Requesting...")
            async with session.post(
                target_url,
                params=querystring,
                json=current_payload,
                headers=headers,
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=config.FETCH_TIMEOUT_SECONDS)
            ) as response:

                if response.status >= 400:
                     if response.status == 403: # Target block
                         err_text = await response.text()
                         logging.warning(f"ID {item_id} Page {page_num}: Received 403 Forbidden - Probable Target Block on Proxy IP. {proxy_log_msg}. Response: {err_text[:100]}")
                         raise ProxyError(f"403 Forbidden for {item_id}") # Signal proxy IP issue
                     elif response.status == 407: # Proxy Auth Failure - THIS IS NEWLY HANDLED HERE
                         # This is caught below by ClientHttpProxyError, but double-check just in case
                         # The library *should* raise ClientHttpProxyError for 407.
                         # If it ever *doesn't* (e.g., library bug), this check *might* catch it,
                         # but it's better to rely on the specific exception type.
                         err_text = await response.text()
                         logging.error(f"ID {item_id} Page {page_num}: Received 407 Proxy Auth Required. Invalid Credentials or Proxy Issue. {proxy_log_msg}. Response: {err_text[:100]}")
                         raise ProxyError("407 Proxy Authentication Required")
                    # Handle other specific errors if needed
                     elif response.status == 404:
                         logging.warning(f"ID {item_id}: Received 404 Not Found from API.")
                         if page_num == 1: return [] # Invalid/delisted ID
                         else: break # End of pages
                     else: # Other 4xx/5xx
                         err_text = await response.text()
                         logging.warning(f"ID {item_id} Page {page_num}: HTTP Error {response.status}. {proxy_log_msg}. Response: {err_text[:100]}")
                         response.raise_for_status() # Raise standard ClientResponseError

                # Status is OK (2xx)
                try:
                    response_json = await response.json()
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as json_err:
                     logging.error(f"ID {item_id} Page {page_num}: Failed JSON decode/content type. {proxy_log_msg}. Error: {json_err}")
                     # Try reading raw text...
                     raise ValueError("Failed to parse JSON response") from json_err

                # Process valid JSON
                page_data = response_json.get("data") if isinstance(response_json, dict) else None

                if isinstance(page_data, list):
                    all_sales_data.extend(page_data)
                    logging.debug(f"ID {item_id} Page {page_num}: Added {len(page_data)} sales.")
                    # Check pagination
                    if response_json.get("nextPage") == "Yes" and len(page_data) > 0:
                         current_offset += config.TCGPLAYER_PAGE_LIMIT
                         page_num += 1
                         await asyncio.sleep(0.1)
                    else:
                        logging.debug(f"ID {item_id}: Pagination end detected after page {page_num}.")
                        break # End of pages
                else:
                    logging.warning(f"ID {item_id} Page {page_num}: Response missing 'data' list or invalid format. Resp: {str(response_json)[:200]}")
                    break # Assume end of data

        # --- MODIFIED Exception Handling: Added ClientHttpProxyError check ---
        except (aiohttp.ClientConnectionError,
                aiohttp.ClientProxyConnectionError, # Catches general proxy connection issues
                aiohttp.ServerDisconnectedError,
                asyncio.TimeoutError) as conn_err:
            # These remain indications of likely proxy failure (network level)
            logging.warning(f"ID {item_id} Page {page_num}: Conn/Timeout ({type(conn_err).__name__}). Raising ProxyError. {proxy_log_msg}. Error: {conn_err}")
            raise ProxyError(f"Proxy Conn/Timeout for {item_id}: {conn_err}") from conn_err

        except aiohttp.ClientHttpProxyError as proxy_auth_err:
            # --- *** NEW: Catch specific proxy HTTP errors (like 407) *** ---
            # Check if it's specifically a 407 error
            if proxy_auth_err.status == 407:
                logging.error(f"ID {item_id} Page {page_num}: Proxy Auth Failed (407). Invalid Credentials/Proxy Issue. {proxy_log_msg}. Raising ProxyError.")
                raise ProxyError("407 Proxy Authentication Required") from proxy_auth_err
            else:
                # Handle other potential proxy HTTP errors if needed, otherwise treat as generic proxy issue
                logging.error(f"ID {item_id} Page {page_num}: Unexpected Proxy HTTP Error {proxy_auth_err.status}. Raising ProxyError. {proxy_log_msg}. Error: {proxy_auth_err}")
                raise ProxyError(f"Proxy HTTP Error {proxy_auth_err.status}") from proxy_auth_err

        # --- Other exception handlers remain mostly the same ---
        except aiohttp.ClientResponseError as http_err: # Catches non-403, non-407 errors >= 400 raised by raise_for_status
            logging.error(f"ID {item_id} Page {page_num}: Unrecoverable Target HTTP {http_err.status}. {proxy_log_msg}")
            raise # Re-raise standard exception (target server fault)

        except ValueError as val_err: # Raised by JSON errors etc.
             logging.error(f"ID {item_id} Page {page_num}: Data Error. {proxy_log_msg}. Error: {val_err}")
             raise # Re-raise standard exception

        except Exception as e: # Catch-all
            if isinstance(e, ProxyError): raise # Re-raise already identified ProxyErrors
            logging.error(f"ID {item_id} Page {page_num}: Unexpected fetch error: {e}. {proxy_log_msg}", exc_info=True)
            raise # Re-raise unknown exception

    # --- Return aggregated list of sales dicts ---
    logging.info(f"ID {item_id}: Finished fetch, {len(all_sales_data)} total sales records retrieved.")
    return all_sales_data