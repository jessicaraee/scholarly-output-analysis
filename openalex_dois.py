#Use case: Upload a list of DOIs to query the OpenAlex API to harvest additional publication details

#Import libraries
import pandas as pd
from functools import reduce
import requests
import csv
import json

#Configure query
URL = 'https://api.openalex.org/works/doi:{doi}'
PER_PAGE = 100
MAILTO = "youremail@youremail.com" #Update with email address"
if not MAILTO:
    raise ValueError('Email address needed for polite pool')

params = {
    'mailto': MAILTO,
    'per-page': PER_PAGE,
    'select': 'id, doi, title, display_name, publication_year, type, corresponding_institution_ids, primary_location, open_access, apc_list, apc_paid, authorships, grants, primary_topic',
}

input_file = '/location/input_file.xlsx' #Update with filepath and name
output_file = f'/location/output_file.xlsx' #Update with desired filepath and name
flattened_columns = ['primary_location', 'open_access', 'apc_list', 'apc_paid'] #Update flattened columns to include

#Import spreadsheet of DOIs to dataframe
input_file_df = pd.read_excel(input_file, dtype={'Title': str, 'DOI': str})
print(input_file_df.head())

#Define function
def get_openalex_data(doi):
    try:
        response = requests.get(URL.format(doi=doi), params=params)
        if response.status_code == 200:
            return response.json()

        else:
            print(f'Error: {response.status_code}')
            return None
    except Exception as e:
        print(f'Exception for DOI {doi}: {e}')
        return None

dois = input_file_df['DOI']

openalex_data = []
for doi in dois:
    result = get_openalex_data(doi)
    if result:
        openalex_data.append(result)

#Add results to dataframe
output_openalex_df = pd.DataFrame(openalex_data)
print(output_openalex_df.head())

#Flatten nested fields, add to dataframes, and select columns to include
primary_location_df = pd.json_normalize(openalex_data,
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

open_access_df = pd.json_normalize(openalex_data,
    record_path = None,
    meta = ['id', 'is_oa', 'oa_status', 'any_repository_has_fulltext'],
    record_prefix = 'open_access.',
    errors = 'ignore'
)

open_access_df = open_access_df[
    ['id', 'open_access.is_oa', 'open_access.oa_status', 'open_access.any_repository_has_fulltext']
]

apc_list_df = pd.json_normalize(openalex_data,
    record_path = None,
    meta = ['id', 'apc_list.value_usd'],
    record_prefix = 'apc_list.',
    errors = 'ignore'
)

apc_list_df = apc_list_df[
    ['id', 'apc_list.value_usd']
]

apc_paid_df = pd.json_normalize(openalex_data,
    record_path = None,
    meta = ['id', 'apc_paid.value_usd'],
    record_prefix = 'apc_paid.',
    errors = 'ignore'
)

apc_paid_df = apc_paid_df[
    ['id', 'apc_paid.value_usd']
]

primary_topic_df = pd.json_normalize(openalex_data,
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

id_column = [result['id'] for result in openalex_data]
for df in dfs:
    df['id'] = id_column

#Exclude unnecessary abstract_inverted_index fields that are sometimes returned in bulk
exclude_substring = 'abstract_inverted_index'
dfs = [
    df.drop(columns=df.filter(like=exclude_substring).columns, errors='ignore')
    for df in dfs
]

flattened_df = reduce(
    lambda left, right: pd.merge(
        left, right, on = 'id', how = 'left', suffixes = ('', '_dup')
    ),
    dfs
)

#Remove extraneous columns and merge with flattened data
merged_df = flattened_df.drop(columns=flattened_columns)

#Merge dataframes together on id field
final_df = merged_df.merge(primary_location_df, on = 'id', how = 'left')

final_df.to_excel(output_file)
print((f"Data successfully exported to {output_file}"))
