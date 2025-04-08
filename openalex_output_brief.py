#Use case: Query the OpenAlex API to pull a list of publications by CY and institution ID to combine with Scopus/Web of Science/other output and run back through the API for publication details using openalex_dois.py.

#Import libraries
import pandas as pd
import requests
import csv
import json

#Enter calendar year and institution ID
calendar_year = 2023
institution_id = 'i00000000'

url = 'https://api.openalex.org/works'
#Enter email address
mailto = "youremail@youremail.com"
if not mailto:
    raise ValueError('Email address needed for polite pool')

params = {
    'mailto': mailto,
    'filter': f'authorships.institutions.lineage:{institution_id},publication_year:{calendar_year}',
    'per-page': 100,
}

#Initialize cursor and loop through pages
cursor = "*"

all_results = []
count_api_queries = 0

while cursor:
    params["cursor"] = cursor
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("Error")
        break
    this_page_results = response.json()['results']
    for result in this_page_results:
        
        #Remove 'https://' from ID field
        if 'id' in result:
            result['id'] = result['id'].split('/')[-1]

        #Remove 'https://' from DOI field if needed for matching later
        if 'doi' in result and result['doi'].startswith("https://"):
            result['doi'] = result['doi'][len("https://"):]
        
        #Store results in list
        all_results.append(result)
    count_api_queries += 1

    #Update cursor, using the response's `next_cursor` metadata field
    cursor = response.json()['meta']['next_cursor']
print(f"{count_api_queries} API queries, {len(all_results)} results.")

#Add results to dataframe
output_openalex_df = pd.DataFrame(all_results)
print(output_openalex_df)

export_filename = f'/filepath/openalexoutput_{institution_id}_CY{calendar_year}.xlsx'

#Identify columns to include and export to XLSX
output_openalex_df.to_excel(export_filename, columns=['id', 'doi', 'title', 'display_name', 'corresponding_institution_ids'])
