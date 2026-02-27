#Use case: Use data exported from openalex_output.py to query OpenAlex, ROR, and Crossref to harvest additional funders data.

#Import libraries
import pandas as pd
import requests
import pyalex
from pyalex import Funders
from tqdm import tqdm
import time
import json
import os

#Configure query
API_KEY = "key"
MAILTO = "youremail@youremail.com"
if not MAILTO:
    raise ValueError('Email address needed for polite pool')
GEONAMES_USERNAME = "username"

#Configure APIs
pyalex.config.api_key = API_KEY
pyalex.config.email = MAILTO
pyalex.config.max_retries = 5
pyalex.config.timeout = 30
DELAY = 0.2

#Configure files
INPUT_FILE = f"/filepath/filename.xlsx"
OUTPUT_FILE = f"/filepath/filename_funders.xlsx"

#Caches
openalex_cache = {}
ror_cache = {}
crossref_cache = {}
geonames_cache = {}

#Load data
df = pd.read_excel(INPUT_FILE)
original_row_count = len(df)

#Define helper functions
def join_pipe(values):
    return " | ".join([str(v) for v in values if v not in [None, "", "nan"]])

def clean_ror(value):
    if not value:
        return None
    return value.replace("https://ror.org/", "").strip()

def clean_doi(value):
    if not value:
        return None
    return value.replace("https://doi.org/", "").strip()

def extract_openalex_ids(cell_value):
    if pd.isna(cell_value):
        return []

    if isinstance(cell_value, list):
        ids = []
        for item in cell_value:
            if isinstance(item, dict) and item.get("id"):
                ids.append(item["id"].replace("https://openalex.org/", ""))
        return ids

    cell_str = str(cell_value).strip()
    if cell_str in ["", "[]"]:
        return []

    if cell_str.startswith("["):
        try:
            data = json.loads(cell_str)
            ids = []
            for item in data:
                if isinstance(item, dict) and item.get("id"):
                    ids.append(item["id"].replace("https://openalex.org/", ""))
            return ids
        except Exception as e:
            print("JSON parsing failed:", e)
            return []

    return [v.replace("https://openalex.org/", "").strip() for v in cell_str.split(" | ") if v.strip()]

#Define API functions
def fetch_openalex_funder(funder_id):
    if funder_id in openalex_cache:
        return openalex_cache[funder_id]

    try:
        funder = Funders()[funder_id]
        openalex_cache[funder_id] = funder
        time.sleep(DELAY)
        return funder
    except Exception as e:
        print("OpenAlex error:", e)
        openalex_cache[funder_id] = None
        return None

def fetch_ror(ror_id):
    if ror_id in ror_cache:
        return ror_cache[ror_id]

    try:
        url = f"https://api.ror.org/v2/organizations/{ror_id}"
        r = requests.get(url)
        data = r.json() if r.status_code == 200 else None
        ror_cache[ror_id] = data
        time.sleep(DELAY)
        return data
    except:
        ror_cache[ror_id] = None
        return None

def fetch_crossref(doi):
    if doi in crossref_cache:
        return crossref_cache[doi]

    try:
        url = f"https://data.crossref.org/fundingdata/funder/{doi}"
        r = requests.get(url)
        data = r.json() if r.status_code == 200 else None
        crossref_cache[doi] = data
        time.sleep(DELAY)
        return data
    except:
        crossref_cache[doi] = None
        return None

#Run query
results = []

for _, row in tqdm(df.iterrows(), total=len(df)):
    funder_ids = extract_openalex_ids(row.get("funders"))

    #OpenAlex
    display_names, alt_titles, country_codes, descriptions, homepage_urls = [], [], [], [], []
    ror_ids, dois = [], []

    for fid in funder_ids:
        data = fetch_openalex_funder(fid)
        if not data:
            continue

        display_names.append(data.get("display_name"))
        alt_titles.append(",".join(data.get("alternate_titles", [])))
        country_codes.append(data.get("country_code"))
        descriptions.append(data.get("description"))
        homepage_urls.append(data.get("homepage_url") or "")

        ror = data.get("ids", {}).get("ror")
        doi = data.get("ids", {}).get("doi")
        if ror: ror_ids.append(clean_ror(ror))
        if doi: dois.append(clean_doi(doi))

    #ROR
    ror_types, ror_country, ror_lat, ror_lng, ror_city, ror_locations = [], [], [], [], [], []

    for rid in ror_ids:
        ror_data = fetch_ror(rid)
        if not ror_data:
            continue

        ror_types.append(",".join(ror_data.get("types", [])))
        locations = ror_data.get("locations", [])
        if locations:
            loc = locations[0]
            geo = loc.get("geonames_details", {})
            ror_country.append(geo.get("country_code", ""))
            ror_lat.append(geo.get("lat", ""))
            ror_lng.append(geo.get("lng", ""))
            ror_city.append(loc.get("name", ""))
            ror_locations.append(loc.get("url", ""))

    #Crossref and Geonames
    cross_country, cross_name, cross_type, cross_subtype, cross_region = [], [], [], [], []
    cross_state_uri, cross_state_name, cross_state_country = [], [], []

    for doi in dois:
        cr = fetch_crossref(doi)
        if not cr:
            continue

        cross_country.append(cr.get("address", {}).get("postalAddress", {}).get("addressCountry", ""))
        cross_name.append(cr.get("prefLabel", {}).get("Label", {}).get("literalForm", {}).get("content", ""))
        cross_type.append(cr.get("fundingBodyType", ""))
        cross_subtype.append(cr.get("fundingBodySubType", ""))
        cross_region.append(cr.get("region", ""))

        state_url = cr.get("state", {}).get("resource")
        cross_state_uri.append(state_url or "")

        if state_url in geonames_cache:
            geo = geonames_cache[state_url]
        elif state_url:
            geonames_id = state_url.rstrip("/").split("/")[-1]
            try:
                geo_req = requests.get(
                    f"http://api.geonames.org/getJSON?geonameId={geonames_id}&username={GEONAMES_USERNAME}"
                )
                geo = geo_req.json() if geo_req.status_code == 200 else {}
            except:
                geo = {}
            geonames_cache[state_url] = geo
        else:
            geo = {}

        cross_state_name.append(geo.get("name", ""))
        cross_state_country.append(geo.get("countryCode", ""))

    results.append({
        "OpenAlex_Funder_IDs": join_pipe(funder_ids),
        "OpenAlex_DisplayName": join_pipe(display_names),
        "OpenAlex_AlternateTitles": join_pipe(alt_titles),
        "OpenAlex_CountryCode": join_pipe(country_codes),
        "OpenAlex_Description": join_pipe(descriptions),
        "OpenAlex_HomepageURL": join_pipe(homepage_urls),
        "OpenAlex_ROR": join_pipe(ror_ids),
        "FunderDOI": join_pipe(dois),
        "ROR_Types": join_pipe(ror_types),
        "ROR_Locations": join_pipe(ror_locations),
        "ROR_CountryCode": join_pipe(ror_country),
        "ROR_Lat": join_pipe(ror_lat),
        "ROR_Long": join_pipe(ror_lng),
        "ROR_City": join_pipe(ror_city),
        "Crossref_Country": join_pipe(cross_country),
        "Crossref_Name": join_pipe(cross_name),
        "Crossref_Type": join_pipe(cross_type),
        "Crossref_Subtype": join_pipe(cross_subtype),
        "Crossref_Region": join_pipe(cross_region),
        "Crossref_StateURI": join_pipe(cross_state_uri),
        "Crossref_StateName": join_pipe(cross_state_name),
        "Crossref_StateCountry": join_pipe(cross_state_country),
    })

#Change returned URLs to text and export data
new_cols_df = pd.DataFrame(results)
df_final = pd.concat([df.reset_index(drop=True), new_cols_df], axis=1)

assert len(df_final) == original_row_count

url_columns = [
    "OpenAlex_HomepageURL",
    "ROR_Locations",
    "Crossref_StateURI"
]

for col in url_columns:
    if col in df_final.columns:
        df_final[col] = df_final[col].apply(lambda x: f"'{str(x)}" if pd.notna(x) else "")

df_final.to_excel(OUTPUT_FILE, index=False)

print("Done! File exported to {OUTPUT_FILE}.")
