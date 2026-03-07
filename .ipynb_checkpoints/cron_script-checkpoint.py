import requests
from datetime import datetime, timezone
import os
import pandas as pd
import psycopg2
import sys
import time

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
def fetch_data(token):
    data = {}
    try:
        
        url = "https://opensky-network.org/api/states/all"

        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        response = requests.get(url, headers=headers)
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

def push_data(icaovals, data, DB_URL):
    return_val = 0
    if not data:
        return 4
    filtdata =  [d for d in data['states'] if d[0] in icaovals]
    print(f"{len(filtdata)} aircrafts' state vectors will be saved.")
    
    try:
        conn = psycopg2.connect(DB_URL,connect_timeout=30)
        cur = conn.cursor()
        insert_query = """
        INSERT INTO aircraft_states (
            snapshot_id,
            icao24,
            callsign,
            origin_country,
            time_position,
            last_contact,
            longitude,
            latitude,
            baro_altitude,
            geo_altitude,
            on_ground,
            velocity,
            true_track,
            vertical_rate,
            squawk,
            spi,
            position_source,
            category
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        cur.execute("insert into state_snapshots (opensky_timestamp) values (%s) returning id", (datetime.fromtimestamp(data['time'], tz = timezone.utc),) )

        snapshot_id=cur.fetchone()[0]
        print("snapshot id is",snapshot_id)
        for row in filtdata:
            cur.execute(insert_query, (
                snapshot_id,
                row[0],                          # icao24
                row[1].strip() if row[1] else None,
                row[2],
                datetime.fromtimestamp(row[3], tz = timezone.utc) if row[3] else None,
                datetime.fromtimestamp(row[4], tz = timezone.utc) if row[4] else None,
                row[5],
                row[6],
                row[7],
                row[13],                         # geo_altitude
                row[8],
                row[9],
                row[10],
                row[11],
                row[14],
                row[15],
                row[16],
                row[17] if len(row) >17 else None
            ))

        conn.commit()
        print(f"data successfully written to postgres")
    except Exception as e:
        print(f"error occured: {e}, couldnt send data")
        return_val = 3
        if 'conn' in locals() and conn is not None:
            conn.rollback()
    finally:
        if 'cur' in locals() and cur is not None:
            cur.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
    return return_val



if __name__ == "__main__":
    
    fleet = pd.read_csv('ME3_fleet.csv')
    icaovals = set(fleet['icao24'])
    i=0
    
    if "DB_URL" in os.environ:
        DB_URL=os.environ["DB_URL"]
    else:
        print("url for database to connect to not specified, aborting")
        sys.exit(2)
    finalret = 0
    while i in range(30):
        if(i%10==0):
            token = update_token()
            if token:
                print("received token correctly, fetching data")
            else:
                print("token is empty, aborting data fetch")
                sys.exit(1)
        
        data = fetch_data(token)
        if data:
            print("data fetch successful at time:", data['time'], datetime.fromtimestamp(data['time'], tz= timezone.utc), len(data['states']), "state vectors received")
        else:
            print("received no data in this iteration")
        success = push_data(icaovals, data, DB_URL)
        if success !=0:
            finalret= success
        time.sleep(120)
    
    sys.exit(finalret)

