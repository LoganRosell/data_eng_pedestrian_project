import pandas as pd
import requests
import time
import os

API_KEY = ""

output_path = "/Users/georgiakirkpatrick/Documents/Willamette_Courses/Spring_Semester/DS_503_Fundamentals_of_Data_Engineering/Project/master_census_population_2018_2023.csv"

years = [2018, 2019, 2021, 2022, 2023]

states_city_dict = {
    "02": "03000", "04": "73000", 
    "06": "26000,39682,44000,48872,67000,68154,70098,82996,83668,84410,00562,06000,64000,66000,68000,69070", 
    "08": "20000,07850", "09": "73000", "11": "50000", 
    "12": "16725,25175,24000,53000,71000,76600", "13": "69000,04000,49000", 
    "17": "14000,54885", "21": "48006", "24": "69925", 
    "25": "11000,07000,62535", "26": "03000", "27": "43000", 
    "29": "15670,38000,39044", "31": "37000", "32": "40000", 
    "34": "33250,36000", "35": "02000", "36": "51000", 
    "37": "01520,12000,19000", "39": "77000,18000", "40": "55000", 
    "41": "59000,23850", "42": "06088,41216,60000,32800", "47": "40000", 
    "48": "05000,24000,41448,65000,35000", "51": "01000,67000,03000", 
    "53": "63000,70000,05210", "55": "53000,48000"
}

all_data_frames = []

for year in years:
    print(f"\n--- Fetching Year: {year} ---")
    year_results = []
    
    for state_fips, place_list in states_city_dict.items():
        # Clean spaces from place_list to prevent API errors
        clean_places = place_list.replace(' ', '')
        url = f"https://api.census.gov/data/{year}/acs/acs1?get=NAME,B01003_001E&for=place:{clean_places}&in=state:{state_fips}&key={API_KEY}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                year_results.extend(data[1:]) # Skip headers
                print(f"  [OK] State {state_fips}")
            elif response.status_code == 204:
                print(f"  [Empty] State {state_fips} (Check pop > 65k)")
            else:
                print(f"  [Error {response.status_code}] State {state_fips}")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  [Fail] State {state_fips}: {e}")

    # Convert this specific year to a DataFrame and add the Year column
    if year_results:
        temp_df = pd.DataFrame(year_results, columns=['name', 'population', 'fips_state_id', 'fips_place_id'])
        temp_df['year'] = year
        all_data_frames.append(temp_df)

# 2. Combine all years into one master table
if all_data_frames:
    master_df = pd.concat(all_data_frames, ignore_index=True)
    
    # Reorder columns to match your request
    master_df = master_df[['year', 'name', 'population', 'fips_state_id', 'fips_place_id']]
    
    # Save to CSV
    master_df.to_csv(output_path, index=False)
    print(f"\nSUCCESS: Master file saved with {len(master_df)} total rows.")
