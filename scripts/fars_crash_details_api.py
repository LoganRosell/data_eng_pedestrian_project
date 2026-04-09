import numpy as np
import pandas as pd
import requests
import json
import os

case = 00000
state = 1
year = 2018

API_endpoint = f"https://crashviewer.nhtsa.dot.gov/crashviewer/CrashAPI/crashes/GetCaseDetails?stateCase={case}&caseYear={year}&state={state}&format=json"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
           'Accept-Language': 'en-US,en;q=0.9'}

directory = "pedestrian_project/crash_detail_jsons"
file_name = "data.json"
file_path = os.path.join(directory, file_name)

# GET request
def get_case_details():
    try:
        # Make a GET request to the API endpoint using requests.get()
        response = requests.get(API_endpoint, headers=headers)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            details = response.json()
            return details
        else:
            print('Error:', response.status_code)
            return None

    except requests.exceptions.RequestException as e:
        # Handle any network-related errors or exceptions
        print('Error:', e)
        return None

#############################################################################

print('making API call...')    

# API call
data = get_case_details()

if data:
    print('response recieved. saving to file...')

    # Save with indentation for readability
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
else:
    print('Failed to fetch crash details from API.')

print('done.')