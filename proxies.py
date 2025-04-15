# # proxies.py
# import json
# import logging
# from typing import List, Optional, Dict, Union
# from urllib.parse import quote
# import config # Import project config

# # Define type alias for Proxy Dictionary
# ProxyDict = Dict[str, Union[str, int, None]]

# def load_proxies() -> List[ProxyDict]:
#     """
#     Loads and formats proxies based on config.py settings.
#     Filters for:
#         - 'HTTP Proxy (SSL)' technology name.
#         - Server location in the 'United States'.
#     Returns a list of proxy dictionaries or an empty list if disabled/error.
#     """
#     if not config.USE_PROXIES:
#         logging.info("Proxy usage is disabled in config.")
#         return []
#     if not config.PROXY_FILE:
#         logging.warning("PROXY_FILE not set in config, cannot load proxies.")
#         return []

#     formatted_proxies: List[ProxyDict] = []
#     try:
#         with open(config.PROXY_FILE, 'r') as file:
#             data = json.load(file)
#         logging.info(f"Loaded raw server data from {config.PROXY_FILE}")
#     except Exception as e:
#         logging.error(f"Error reading or parsing proxy file {config.PROXY_FILE}: {e}")
#         return []

#     username = config.PROXY_USERNAME
#     password = config.PROXY_PASSWORD
#     port = config.PROXY_PORT

#     if username is None or password is None:
#         logging.warning("Proxy credentials missing. Proxies configured without authentication.")

#     count = 0
#     # Iterate through each server in the JSON data
#     for server in data:
#         # Basic checks for required keys
#         if "hostname" not in server or "technologies" not in server or "locations" not in server:
#             continue # Skip if essential info is missing

#         # --- Check 1: Technology Filter ---
#         has_ssl_proxy_tech = False
#         if isinstance(server["technologies"], list):
#             has_ssl_proxy_tech = any(
#                 isinstance(tech, dict) and tech.get("name") == "HTTP Proxy (SSL)"
#                 for tech in server["technologies"]
#             )

#         # --- Check 2: Location Filter ---
#         is_in_us = False
#         if isinstance(server["locations"], list):
#             for loc in server["locations"]:
#                 # Navigate the nested structure safely using .get()
#                 country_info = loc.get("country")
#                 if isinstance(country_info, dict) and country_info.get("name") == "United States":
#                     is_in_us = True
#                     break # Found a US location, no need to check others for this server

#         # --- Combine Filters ---
#         if has_ssl_proxy_tech and is_in_us:
#             # Both conditions met, format and add the proxy
#             safe_username = quote(username, safe='') if username else None
#             safe_password = quote(password, safe='') if password else None
#             proxy_info: ProxyDict = {
#                 'protocol': 'https',
#                 'host': server["hostname"],
#                 'port': port,
#                 'username': safe_username,
#                 'password': safe_password,
#                 'location': 'United States' # Optional: Store location for logging/debugging
#             }
#             formatted_proxies.append(proxy_info)
#             count += 1
#         elif has_ssl_proxy_tech and not is_in_us:
#              logging.debug(f"Server {server.get('hostname', 'N/A')} skipped: Has SSL Proxy, but not located in the United States.")


#     logging.info(f"Found {count} servers matching criteria: 'HTTP Proxy (SSL)' tech AND located in 'United States'. Formatted with port {port}.")
#     if not formatted_proxies and config.USE_PROXIES:
#          logging.warning(f"No servers matched BOTH criteria in {config.PROXY_FILE}.")

#     return formatted_proxies


import csv
import logging
from typing import List, Dict, Union, Optional
from urllib.parse import quote
import config  # Import project config

# Define type alias for Proxy Dictionary
ProxyDict = Dict[str, Union[str, int, None]]

def load_proxies_csv() -> List[ProxyDict]:
    """
    Loads and formats proxies from a CSV file as defined in config.PROXY_FILE.
    Expects the CSV file to be formatted with columns:
        host,port,username,password
    Returns a list of proxy dictionaries or an empty list if disabled/error.
    """
    if not config.USE_PROXIES:
        logging.info("Proxy usage is disabled in config.")
        return []
    if not config.PROXY_FILE:
        logging.warning("PROXY_FILE not set in config, cannot load proxies.")
        return []

    formatted_proxies: List[ProxyDict] = []
    try:
        with open(config.PROXY_FILE, 'r', newline='') as file:
            # Assuming the CSV file has no header; if it does, remove fieldnames or use csv.DictReader(file)
            reader = csv.DictReader(file, fieldnames=["host", "port", "username", "password"])
            for row in reader:
                # Convert port to int (assuming it is numeric) and safely encode the username/password if needed.
                try:
                    port = int(row["port"])
                except ValueError:
                    logging.warning(f"Skipping proxy with invalid port value: {row['port']}")
                    continue

                # URL-encode credentials if provided
                username = row["username"]
                password = row["password"]

                safe_username = quote(username, safe='') if username else None
                safe_password = quote(password, safe='') if password else None

                proxy_info: ProxyDict = {
                    'protocol': 'https',      # or change to 'http' if needed
                    'host': row["host"],
                    'port': port,
                    'username': safe_username,
                    'password': safe_password,
                    'location': None          # CSV does not include location info, so this is left as None
                }
                formatted_proxies.append(proxy_info)
    except Exception as e:
        logging.error(f"Error reading or parsing CSV proxy file {config.PROXY_FILE}: {e}")
        return []

    logging.info(f"Loaded {len(formatted_proxies)} proxies from CSV.")
    return formatted_proxies
