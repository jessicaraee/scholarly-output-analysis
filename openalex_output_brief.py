#Use case: Query the OpenAlex API to pull a list of publications by CY and institution ID to combine with Scopus/Web of Science/other output and run back through the API for publication details using openalex_dois.py.

#Import libraries
import pandas as pd
import requests
import csv
import json

#Configure query
calendar_year = 2023
institution_id = 'i00000000'

URL = 'https://api.openalex.org/works'
PER_PAGE = 100
MAILTO = "youremail@youremail.com"
if not MAILTO:
    raise ValueError('Email address needed for polite pool')

params = {
    'mailto': MAILTO,
    'filter': f'authorships.institutions.lineage:{institution_id},publication_year:{calendar_year}',
    'per-page': PER_PAGE,
}

export_filename = f'/filepath/openalexoutput_{institution_id}_CY{calendar_year}.xlsx' #Update with desired filepath and name
export_columns = ['id', 'doi', 'title', 'display_name', 'corresponding_institution_ids'] #Update fields to include

#Initialize cursor and loop through pages
cursor = "*"

all_results = []
count_api_queries = 0

while cursor:
    params["cursor"] = cursor
    response = requests.get(URL, params=params)
    if response.status_code != 200:
        print("Error")
        break
    this_page_results = response.json()['results']
    for result in this_page_results:
        
        #Remove 'https://' from ID field
        if 'id' in result:
            result['id'] = result['id'].split('/')[-1]

        #Remove 'https://' from DOI field for easier matching later
        if 'doi' in result and result['doi'].startswith("https://"):
            result['doi'] = result['doi'][len("https://"):]
        
        #Store results in list
        all_results.append(result)
        
    count_api_queries += 1

    #Update cursor using response's 'next_cursor' field
    cursor = response.json()['meta']['next_cursor']
    
print(f"{count_api_queries} API queries, {len(all_results)} results.")

#Add results to dataframe
output_openalex_df = pd.DataFrame(all_results)
print(output_openalex_df)

#Export to XLSX
output_openalex_df.to_excel(export_filename, columns=export_columns)
