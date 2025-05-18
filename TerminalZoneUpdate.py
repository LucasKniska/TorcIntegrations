# Run First
import json
import requests
import pandas as pd
import os
from geopy.distance import distance

production = True

# config
tenant = "torcrobotics.us.accelix.com" if production else "torcroboticssb.us.accelix.com"
site = "def"

# Cookie to the sandbox
sandbox_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"
production_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJmYTJkODc5Mi04ZjFjLTRmZDEtOGExYS04NGY2ZjZhYmU3NjgiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.FgsUCL81lnh6DJv6Ec4fuT5gyNtqKyeFgEx_Etz8CDo"
motive_key = "9e90504a-82f0-4ed4-b54c-ce37f388f211"

headers = {'Content-Type': 'application/json', 'Cookie': production_key if production else sandbox_key}


# City Constants 

# Define cities and their coordinates
# (latitude, longitude)
CITIES = {
    'ACM (American Center for Mobilities, MI)': (42.2381, -83.5537),
    'BCB (Blacksburg, VA)': (37.1897, -80.3921),
    'DFW (Dallas, TX)': (32.9669, -97.2983),
    'UPG (Uvalde Proving Grounds, TX)': (29.1125, -99.7521),
    'PDX (Portland, OR)': (45.5152, -122.6784), 
    'AUS (Austin, TX)': (30.0667, -97.8357),
    'MPG (Michellin Proving Grounds, SC)': (34.3925, -82.0291),
    'ARB (Ann Arbor, MI)': (42.3057, -83.6842) # Need full name of this one   
}

KEYS_TO_CITIES = {
    'ACM (American Center for Mobilities, MI)': 'b709fb0b-c6a8-45ac-a829-ebc73e98ad4d',
    'BCB (Blacksburg, VA)': '351680e3-38fc-481a-b2e4-8a3834006c03',
    'DFW (Dallas, TX)': '9056cfa3-701d-46ed-8aec-37d2b642d2b4', 
    'UPG (Uvalde Proving Grounds, TX)': '8eb92cad-6a68-4dc1-ae6e-58ced7eb34e0', 
    'PDX (Portland, OR)': '83e7a2ec-da8d-4f9c-8a45-a3580bd1ac79',
    'MTL (Montreal, CN)': '2a900522-59bc-4518-ab7a-3f9af8fd5762',
    'AUS (Austin, TX)': 'c5057cf9-7df8-436e-afd3-a243818a6b9b',
    'MPG (Michellin Proving Grounds, SC)': '08203d31-a3b8-4252-9d6e-acb512f0e246',
    'ARB (Ann Arbor, MI)': 'f0d0e7ac-511e-4ae7-a960-7c411eb6c917', # Need full name of this one
}


def get_geolocations():
    """
    Gets all of the freightliners and trailer assets from fluke.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns for each asset:
            - 'c_description': Number of the truck (ex: C19 - Mill Mountain).
            - 'c_assettype': The type of the asset (either 'Freightliner' or 'Trailer').
            - 'id': The unique identifier of the asset.

    """

    # Get the freightliner assets
    url = f'https://{tenant}/api/entities/{site}/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_description"},
            {"name": "id"},
            {"name": "geolocation"},
            {"name": "c_terminalzonedropdown"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"},
                {"name": "c_assettype", "op": "eq", "value": "Freightliner"},
            ],
        },
        "order": [],
        "pageSize": 50,
        "page": 0,
        "fkExpansion": True
    }

    # API
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print("Error: getting Freightliners", flush=True)
        return False
    
    response = response.json()
    dx = response['data']
    pages = response['totalPages']
    for page in range(1, pages):
        data['page'] = page
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error: getting Freightliners", flush=True)
            return False
        dx.extend(response.json()['data'])

        # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    return df


def createTerminalZone(city):

    return {
        "entity": "TerminalZone",
        "id": KEYS_TO_CITIES[city],
        "isDeleted": False,
        "number": 1,
        "title": city
    }

# Function to get nearest city using geopy
def get_nearest_city(location):
    if location is None:
        return None
    try:
        loc = (location['lat'], location['long'])
    except:
        try: 
            loc = (location['lat'], location['lng'])
        except:
            print("Error: Could not process: ", location, flush=True)
            return None
    
    nearest = min(CITIES.items(), key=lambda city: distance(loc, city[1]).km)
    return nearest[0]

# Post the 'nearest_city' field to the associated 'id' for each truck
def post_nearest_city(truck):
    
    ############################# USED FOR TESTING ON C19
    truck["id"] = '6b741a62-a59d-4fbe-89d9-f4996bbb5170'

    url = f'https://{tenant}/api/entities/{site}/Assets/{truck["id"]}'
    
    data = {
        "properties": {
            "c_terminalzonedropdown": createTerminalZone(truck['nearest_city'])
        },
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error: updating {truck['id']}", flush=True)
        return False
    
    return True


if __name__ == "__main__":
    trucks = get_geolocations()

    # Apply the function to each row
    trucks['nearest_city'] = trucks['geolocation'].apply(get_nearest_city)

    for i, truck in trucks.iterrows():

        if(truck['nearest_city'] is not None):
            post_nearest_city(truck)

    print("Run Complete", flush=True)