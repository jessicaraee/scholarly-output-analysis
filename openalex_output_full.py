#Use case: Query the OpenAlex API to pull a detailed list of publications by CY and institution ID, customizing included fields as needed.

#Import libraries
import pandas as pd
from functools import reduce
import requests
import csv
import json
import time

#Configure query
calendar_year = 2023
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
FLATTENED_COLUMNS = ['primary_location', 'open_access', 'apc_list', 'apc_paid', 'primary_topic']

#Initialize cursor and loop through pages, allowing for wait times if errors
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

#Add results to dataframe
output_openalex_df = pd.DataFrame(all_results)
print(output_openalex_df.head())

#Flatten nested fields, add to dataframes, and select columns to include
primary_location_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'is_oa', 'landing_page_url',
    ['source', 'display_name'], ['source', 'issn_l'], ['source', 'is_oa'],
    ['source', 'is_in_doaj'], ['source', 'host_organization_name'],
    ['source', 'host_organization_lineage_names'], ['source', 'type'],
    'is_accepted', 'is_published'],
    record_prefix = 'primary_location.',
    errors = 'ignore'
)

primary_location_df = primary_location_df[
    ['id', 'primary_location.is_oa', 'primary_location.landing_page_url', 'primary_location.source.display_name',
    'primary_location.source.issn_l', 
    'primary_location.source.is_oa', 'primary_location.source.is_in_doaj',
    'primary_location.source.host_organization_name', 'primary_location.source.host_organization_lineage_names',
    'primary_location.source.type', 'primary_location.is_accepted', 'primary_location.is_published']
]

open_access_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'is_oa', 'oa_status', 'any_repository_has_fulltext'],
    record_prefix = 'open_access.',
    errors = 'ignore'
)

open_access_df = open_access_df[
    ['id', 'open_access.is_oa', 'open_access.oa_status', 'open_access.any_repository_has_fulltext']
]

apc_list_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'apc_list.value_usd'],
    record_prefix = 'apc_list.',
    errors = 'ignore'
)

apc_list_df = apc_list_df[
    ['id', 'apc_list.value_usd']
]

apc_paid_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'apc_paid.value_usd'],
    record_prefix = 'apc_paid.',
    errors = 'ignore'
)

apc_paid_df = apc_paid_df[
    ['id', 'apc_paid.value_usd']
]

primary_topic_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'display_name', ['subfield', 'display_name'],
    ['field', 'display_name'], ['domain', 'display_name']],
    record_prefix = 'primary_topic.',
    errors = 'ignore'
)

primary_topic_df = primary_topic_df[
    ['id', 'primary_topic.display_name', 'primary_topic.subfield.display_name',
    'primary_topic.field.display_name', 'primary_topic.domain.display_name']
]

#Merge dataframes and export results
dfs = [output_openalex_df, primary_location_df, open_access_df, primary_topic_df]

exclude_substring = 'abstract_inverted_index' #Exclude abstract_inverted_index fields that result in extra blank columns
dfs = [
    df.drop(columns=df.filter(like=exclude_substring).columns, errors='ignore')
    for df in dfs
]

flattened_df = reduce(
    lambda left, right: pd.merge(
        left, right, on='id', how='left'
    ),
    dfs
)

final_df = flattened_df.drop(columns=FLATTENED_COLUMNS) #Remove any extra columns that may be left and merge with flattened data

print("\nFinal DataFrame Before Export:")
print(final_df.head())
print(final_df.info())
print(final_df.dtypes)

final_df.to_excel(OUTPUT_FILE)
print((f"Data successfully exported to {OUTPUT_FILE}."))
