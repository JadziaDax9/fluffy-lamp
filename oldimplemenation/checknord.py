import json

# Load the JSON data
with open('nordvpnserver.json', 'r') as file:
    data = json.load(file)

# List to store hostnames of servers with HTTP proxy technologies
proxy_hostnames = []

# Iterate through each server
for server in data:
    # Check if the server has technologies
    if "technologies" in server:
        # Check if any technology is an HTTP proxy
        has_http_proxy = any(
            tech.get("identifier", "").startswith("proxy_ssl") 
            for tech in server["technologies"]
        )
        
        # If it has an HTTP proxy technology, add its hostname to our list
        if has_http_proxy:
            proxy_hostnames.append(server["hostname"])

# Print the hostnames
print("Servers with HTTP proxy technologies:")
for hostname in proxy_hostnames:
    print(hostname)