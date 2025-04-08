#Use case: Query the OpenAlex API to pull a detailed list of publications by CY and institution ID, customizing included fields as needed.

#Import libraries
import pandas as pd
from functools import reduce
import requests
import csv
import json

#Enter calendar year, institution ID, and email address
calendar_year = 2024
institution_id = 'i00000000'
mailto = 'youremail@youremail.com'

url = 'https://api.openalex.org/works'
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
        print(f'Error: {response.status_code} - {response.text}')
        break
    
    this_page_results = response.json()['results']
    
    for result in this_page_results:
        #Remove 'https://' from ID field
        if 'id' in result:
            result['id'] = result['id'].split('/')[-1]
        
        #Store results in list
        all_results.append(result)

    count_api_queries += 1

    #Update cursor
    cursor = response.json()['meta']['next_cursor']

print(f"{count_api_queries} API queries, {len(all_results)} results.")

#Add results to dataframe
output_openalex_df = pd.DataFrame(all_results)
print(output_openalex_df.head())

#Flatten nested fields, add to dataframes, and select columns to include

primary_location_df = pd.json_normalize(all_results,
    record_path = None,
    meta = ['id', 'is_oa', 'landing_page_url',
    ['source', 'display_name'], ['source', 'issn_l'],
    ['source', 'issn'], ['source', 'is_oa'],
    ['source', 'is_in_doaj'], ['source', 'host_organization_name'],
    ['source', 'host_organization_lineage_names'], ['source', 'type'],
    'is_accepted', 'is_published'],
    record_prefix = 'primary_location.',
    errors = 'ignore'
)

primary_location_df = primary_location_df[
    ['id', 'primary_location.is_oa', 'primary_location.landing_page_url', 'primary_location.source.display_name',
    'primary_location.source.issn_l', 'primary_location.source.issn',
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

dfs = [output_openalex_df, primary_location_df, open_access_df, apc_list_df, apc_paid_df, primary_topic_df]

#Exclude unnecessary abstract_inverted_index fields
exclude_substring = 'abstract_inverted_index'
dfs = [
    df.drop(columns=df.filter(like=exclude_substring).columns, errors='ignore')
    for df in dfs
]

#Merge dataframes on id field
flattened_df = reduce(
    lambda left, right: pd.merge(
        left, right, on = 'id', how = 'left', suffixes = ('', '_dup')
    ),
    dfs
)

#Remove extraneous columns and merge with flattened data
final_df = flattened_df.drop(columns=['primary_location', 'open_access', 'apc_list', 'apc_paid', 'primary_topic'])

final_df.to_excel(f'/filepath/openalexoutput_{institution_id}_CY{calendar_year}_flattenedFINAL.xlsx')
