
import pandas as pd
import requests
import time

API_KEY = "861cdf2e46e3c94ad115c18b40992db1a93660c2"

output_path = "/Users/georgiakirkpatrick/Documents/Willamette_Courses/Spring_Semester/DS_503_Fundamentals_of_Data_Engineering/Project/census_results_2024.csv"

states_to_query = ["02", "04", "06", "08", "09", "11", "12", "13", "17", "21", "24", "25", "26", "27", "29", "31", "32", "34", "35", "36", "37", "39", "40", "41", "42", "47", "48", "51", "53", "55"] 

states_city_dict = {
    "02": "03000", 
    "04": "73000", 
    "06": "26000, 39682, 44000, 48872, 67000, 68154, 70098, 82996, 83668, 84410, 00562, 06000, 64000, 66000, 68000, 69070", 
    "08": "20000, 07850", 
    "09": "73000", 
    "11": "50000", 
    "12": "16725, 25175, 24000, 53000, 71000, 76600", 
    "13": "69000, 04000, 49000", 
    "17": "14000, 54885", 
    "21": "48006", 
    "24": "69925", 
    "25": "11000, 07000, 62535", 
    "26": "03000", 
    "27": "43000", 
    "29": "15670, 38000, 39044", 
    "31": "37000", 
    "32": "40000", 
    "34": "33250, 36000", 
    "35": "02000", 
    "36": "51000", 
    "37": "01520, 12000, 19000", 
    "39": "77000, 18000", 
    "40": "55000", 
    "41": "59000, 23850", 
    "42": "06088, 41216, 60000, 32800", 
    "47": "40000", 
    "48": "05000, 24000, 41448, 65000, 35000", 
    "51": "01000, 67000, 03000", 
    "53": "63000, 70000, 05210", 
    "55": "53000,48000"
}

all_results = []

for state_fips, place_list in states_city_dict.items():
    url = f"https://api.census.gov/data/2024/acs/acs1?get=NAME,B01003_001E&for=place:{place_list}&in=state:{state_fips}&key={API_KEY}"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            all_results.extend(data[1:])
            print(f"Successfully retrieved data for State {state_fips}")
        elif response.status_code == 204:
            print(f"No data found for State {state_fips} (Check if these are < 65k population)")
        else:
            print(f"Error for State {state_fips}: {response.status_code}")
            
        time.sleep(0.5)
        
    except Exception as e:
        print(f"Failed to query State {state_fips}: {e}")

if all_results:
    final_df = pd.DataFrame(all_results, columns=['NAME', 'POPULATION', 'state', 'place'])
    final_df.to_csv(output_path, index=False)
    print(f"\nSuccess! Total cities captured: {len(final_df)}")
    print(f"File saved to: {output_path}")

states_county_dict = {

}