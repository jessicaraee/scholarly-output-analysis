#Use case: Query the OpenAlex API to pull a list of publications by CY and institution ID to combine with Scopus/Web of Science/other output and run back through the API for publication details using openalex_dois.py.

#Import libraries
import pandas as pd
import requests
import csv
import json
import time

#Configure query
calendar_year = 2024
institution_id = 'i00000000'

URL = 'https://api.openalex.org/works?data-version=2'
PER_PAGE = 100
MAILTO = "youremail@youremail.com"
if not MAILTO:
    raise ValueError('Email address needed for polite pool')

params = {
    'mailto': MAILTO,
    'filter': f'authorships.institutions.lineage:{institution_id},publication_year:{calendar_year}',
    'per-page': PER_PAGE,
}

#Configure files
OUTPUT_FILE = f'/filepath/openalexoutput_{institution_id}_CY{calendar_year}.xlsx'
OUTPUT_COLUMNS = ['id', 'doi', 'title', 'display_name', 'corresponding_institution_ids']

#Fetch data, allowing for retries if errors
cursor = "*"
all_results = []
count_api_queries = 0
max_retries = 5
polite_delay = 1.2

while cursor:
    params["cursor"] = cursor
    retries = 0

    while retries < max_retries:
        response = requests.get(URL, params=params)

        #Too many requests
        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry after", 5))
            print(f"Rate limited! Waiting {wait_time} seconds.")
            time.sleep(wait_time)
            retries += 1
            continue

        #Transient error
        if response.status_code >= 500:
            print(f"Server error {response.status_code}. Retry #{retries+1}/{max_retries}.")
            time.sleep(3)
            retries += 1
            continue

        #Other errors
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            response.raise_for_status()

        data = response.json()
        this_page_results = data.get('results', [])
        
        for result in this_page_results:
            if 'id' in result:
                result['id'] = result['id'].split('/')[-1] #Remove 'https://' from ID field

            all_results.append(result)

        cursor = data.get('meta', {}).get('next_cursor') #Update cursor

        count_api_queries += 1

        time.sleep(polite_delay)
        break
    else:
        print("Too many failed attempts! Stopping process.")
        break

print(f"{count_api_queries} API queries, {len(all_results)} results.")

#Add results to dataframe and export
output_openalex_df = pd.DataFrame(all_results)
print(output_openalex_df)

output_openalex_df.to_excel(OUTPUT_FILE, columns=OUTPUT_COLUMNS)
