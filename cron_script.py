import requests
from datetime import datetime, timezone
import os
def update_token():
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    if "CLIENT_ID" in os.environ:
        CLIENT_ID=os.environ["CLIENT_ID"]
    else:
        print("client id not specified to connect to opensky api, aborting")
        return ""
    if "CLIENT_SECRET" in os.environ:
        CLIENT_SECRET=os.environ["CLIENT_SECRET"]
    else:
        print("client secret key not specified to connect to opensky api, aborting")
        return ""
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    token = ""
    try:
        print("log debug", f"Sent\tPOST\t{url}\t''\t''\t{data}")
        response = requests.post(url, data=data, timeout=30)
        try:
            response.raise_for_status()
            token = response.json()["access_token"]
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}, response text received: {response.text}, response status code: {response.status_code}")
    except requests.exceptions.Timeout:
        print("Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"Network-related error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    
    return token
def fetch_data():
    data = {}
    try:
        # token = update_token()
        # if token:
        #     print("received token correctly, fetching data")
        # else:
        #     print("token is empty, aborting data fetch")
        #     return
        
        begin_time=datetime.now(timezone.utc)
        params = {
            # "time": int(begin_time.timestamp()),
        }
        
        url = "https://opensky-network.org/api/states/all"
        
        headers = {
            # "Authorization": f"Bearer {token}"
        }
        
        print("log debug", f"Sent\tGET\t{url}\t{headers}\t{params}\t''")
        response = requests.get(url, headers=headers, params=params)
        print("log debug", f"RECV\t{response.status_code}\t{response.headers}")
        try:
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}, response text received: {response.text}, response status code: {response.status_code}")
    except requests.exceptions.Timeout:
        print("Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"Network-related error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return data


if __name__ == "__main__":
    data = fetch_data()
    if data:
        print("data fetch successful at time:", data['time'], datetime.fromtimestamp(data['time'], tz= timezone.utc), len(data['states']), "state vectors received")