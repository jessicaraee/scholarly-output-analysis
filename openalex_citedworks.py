#Use case: Upload a list of works to query the OpenAlex API to harvest publication details for each work cited.

#Import libraries
import requests
import pandas as pd
from collections import Counter

#Configure query
URL = 'https://api.openalex.org/works'
PER_PAGE = 100
MAILTO = "youremail@youremail.com" #Update with email address
if not MAILTO:
    raise ValueError('Email address needed for polite pool')

params = {
    'mailto': MAILTO,
    'per-page': PER_PAGE,
}

input_file = '/filepath/input_file.csv' #Update with filepath and name
output_file = '/filepath/output_file.xlsx' #Update with filepath and name

#Define functions
def api_query_page_results(url, params):
    #Initialize cursor and loop through pages
    cursor = "*"
    all_results = []

    while cursor:
        params["cursor"] = cursor
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print("Error")
            response.raise_for_status()
        this_page_results = response.json()['results']
        for result in this_page_results:
            all_results.append(result)

        #Update cursor
        cursor = response.json()['meta']['next_cursor']
    return all_results

def get_works_from_csv(input_file):
    works = []
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row = {k.strip().lower():v.strip() for k, v in row.items() if k}
                if 'id' in row and row['id']:
                    clean_id = row['id'.replace("https://openalex.org/", "")]
                    works.append({'id': clean_id})
                else:
                    print(f"Warning: Row without 'id' column or empty id: {row}")
        return works
    except FileNotFoundError:
        print(f"Error: CSV file not found at {input_file}")
        return []
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return []

def make_short_id(long_id):
    return long_id.replace("https://openalex.org/", "") if long_id else None

all_references = {}
count_works_retrieved = 0

works_to_collect = get_works_from_csv(input_file)
if not works_to_collect:
    print("No works to collect.")

for work in works_to_collect:
    work_id = work['id']
    params = {
        "mailto": MAILTO,
        "filter": f"cited_by:{work_id}",
        "per-page": PER_PAGE,
        "select": "id,doi,publication_year,title,primary_location,authorships,topics",
        }
    references = api_query_page_results(URL, params)
    all_references[work_id] = references
    count_works_retrieved += len(references)

print(f"Retrieved {count_works_retrieved} works cited by works in {input_file}.")

#Create dataframes
references_data = []
metadata_data = []
citation_counts = Counter()
seen_work_ids = set()

#Write reference pairs
for citing_id, cited_works in all_references.items():
    for w in cited_works:
        cited_id = w.get('id')
        if cited_id:
            references_data.append({
                'citing_paper_id': citing_id,
                'cited_paper_id': make_short_id(cited_id)
            })
            citation_counts.update([cited_id])

#Write metadata
for cited_works in all_references.values():
    for w in cited_works:
        work_id = w.get('id')
        if work_id and work_id not in seen_work_ids and w.get('title') != 'Deleted Work':
            doi = w.get('doi')
            title = w.get('title')
            cul_citation_count = citation_counts[work_id]

            try:
                source = w.get('primary_location', {}).get('source', {})
                source_id = make_short_id(source.get('id'))
                source_issn = source.get('issn_l')
                source_display_name = source.get('display_name')
                source_host_organization = source.get('host_organization')
                source_host_organization_name = source.get('host_organization_name')
            except (AttributeError, KeyError):
                source_id = source_issn = source_display_name = None
                source_host_organization = source_host_organization_name = None

            try:
                topic = w.get('topics', [{}])[0]
                primary_topic_id = make_short_id(topic.get('id'))
                primary_topic_display_name = topic.get('display_name')
            except (IndexError, AttributeError, KeyError):
                primary_topic_id = primary_topic_display_name = None

            metadata_data.append({
                'work_id': make_short_id(work_id),
                'title': title,
                'doi': doi,
                'cul_citation_count': cul_citation_count,
                'source_id': source_id,
                'source_issn': source_issn,
                'source_display_name': source_display_name,
                'source_host_organization': source_host_organization,
                'source_host_organization_name': source_host_organization_name,
                'primary_topic_id': primary_topic_id,
                'primary_topic_display_name': primary_topic_display_name
            })

            seen_work_ids.add(work_id)

#Fill and merge dataframes
df_references = pd.DataFrame(references_data)
df_metadata = pd.DataFrame(metadata_data)
df_combined = df_references.merge(df_metadata, left_on='cited_paper_id', right_on='work_id', how='left')
df_combined.to_excel(output_file, index=False)

print(f"Combined file saved to: {output_file}")
