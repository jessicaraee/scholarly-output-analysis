#Use case: Use data exported from openalex_output.py to query OpenAlex, ROR, Crossref, and GeoNames to harvest additional funder metadata in an unnested, row-by-row format.

#Import libraries
import pandas as pd
import requests
import pyalex
from pyalex import Funders
from tqdm import tqdm
import time
import json
import os
import re

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
award_cache = {}
ror_cache = {}
crossref_cache = {}
geonames_cache = {}

#Federal funder lookup tables
US_FEDERAL_OPENALEX_IDS = {
    #HHS and NIH
    "F4320321285": "Advanced Research Projects Agency for Health (ARPA-H)",
    "F4320320302": "Agency for Healthcare Research and Quality (AHRQ)",
    "F4320319938": "Centers for Disease Control and Prevention (CDC)",
    "F4320320921": "Food and Drug Administration (FDA)",
    "F4320320495": "Health Resources and Services Administration (HRSA)",
    "F4320337351": "National Cancer Institute (NCI)",
    "F4320338384": "National Heart, Lung, and Blood Institute (NHLBI)",
    "F4320338148": "National Institute of Allergy and Infectious Diseases (NIAID)",
    "F4320338128": "National Institute of General Medical Sciences (NIGMS)",
    "F4320338240": "National Institute of Mental Health (NIMH)",
    "F4320338189": "National Institute of Neurological Disorders and Stroke (NINDS)",
    "F4320338304": "National Institute on Aging (NIA)",
    "F4320332161": "National Institutes of Health (NIH)",
    "F4320320501": "Substance Abuse and Mental Health Services Administration (SAMHSA)",

    #Other agencies
    "F4320321301": "Advanced Research Projects Agency-Energy (ARPA-E)",
    "F4320320963": "Air Force Office of Scientific Research (AFOSR)",
    "F4320320959": "Army Research Office (ARO)",
    "F4320320988": "Defense Advanced Research Projects Agency (DARPA)",
    "F4320320971": "Defense Threat Reduction Agency (DTRA)",
    "F4320320995": "Institute of Education Sciences (IES)",
    "F4320321000": "Institute of Museum and Library Services (IMLS)",
    "F4320321018": "National Aeronautics and Space Administration (NASA)",
    "F4320320999": "National Endowment for the Arts (NEA)",
    "F4320321002": "National Endowment for the Humanities (NEH)",
    "F4320321860": "National Institute of Food and Agriculture (NIFA)",
    "F4320321005": "National Institute of Standards and Technology (NIST)",
    "F4320321015": "National Oceanic and Atmospheric Administration (NOAA)",
    "F4320321013": "National Science Foundation (NSF)",
    "F4320320967": "Office of Naval Research (ONR)",
    "F4320320638": "United States Agency for International Development (USAID)",
    "F4320321857": "United States Department of Agriculture (USDA)",
    "F4320308061": "United States Department of Defense (DOD)",
    "F4320319111": "United States Department of Education (ED)",
    "F4320318406": "United States Department of Energy (DOE)",
    "F4320318800": "United States Department of Transportation (DOT)",
    "F4320318855": "United States Department of Veterans Affairs (VA)",
    "F4320321011": "United States Environmental Protection Agency (EPA)",
    "F4320321008": "United States Geological Survey (USGS)"
}

#Federal funder ROR ids (add as needed)
US_FEDERAL_ROR_IDS = {
    "027ka1x80": "National Aeronautics and Space Administration (NASA)",
    "01db37752": "National Institutes of Health (NIH)",
    "021nxhr62": "National Science Foundation (NSF)",
    "0193cq402": "United States Department of Defense (DOD)",
    "0150b0728": "United States Department of Energy (DOE)"
}

#Fallback federal funder regex match patterns (update as needed)
US_FED_REGEX = re.compile(
    r"\b("
    r"AFOSR|Agency for Healthcare Research and Quality|AHRQ|Air Force Office of Scientific Research|"
    r"Advanced Research Projects Agency for Health|ARPA-E|ARPA-H|Army Research Office|ARO|"
    r"Centers for Disease Control|CDC|DARPA|Defense Advanced Research Projects Agency|"
    r"Defense Threat Reduction Agency|Department of Agriculture|Department of Defense|Department of Education|"
    r"Department of Energy|Department of Transportation|Department of Veterans Affairs|DOD|DOE|DTRA|"
    r"Environmental Protection Agency|EPA|FDA|Food and Drug Administration|Health Resources and Services Administration|HRSA|"
    r"IES|IMLS|Institute of Education Sciences|Institute of Museum and Library Services|"
    r"NASA|National Aeronautics and Space Administration|National Cancer Institute|National Endowment for the Arts|"
    r"National Endowment for the Humanities|National Institute [a-zA-Z\s]+|National Institutes of Health|"
    r"National Oceanic and Atmospheric Administration|National Science Foundation|NCI|NEA|NEH|NIFA|NIH|NIST|"
    r"NOAA|NSF|Office of Naval Research|ONR|SAMHSA|Substance Abuse and Mental Health Services Administration|"
    r"U\.S\. Geological Survey|U\.S\. Government|United States Government|US Government|USAID|USDA|USGS|VA"
    r")\b",
    re.IGNORECASE
)

#Load data
df_input = pd.read_excel(INPUT_FILE)
print(f"Loaded {len(df_input):,} rows from {INPUT_FILE}.")

#Define helper functions
def safe_json_load(value):
    if pd.isna(value) or not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []

def clean_id(value):
    if pd.isna(value) or not value:
        return None
    val_str = str(value).strip().rstrip("/")
    if not val_str:
        return None
    return val_str.split("/")[-1]

def clean_ror(value):
    return clean_id(value)

def clean_doi(value):
    return clean_id(value)

def clean_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", str(name)).strip()

def extract_openalex_ids(cell_value):
    if pd.isna(cell_value) or cell_value == "":
        return []

    #Python lists
    if isinstance(cell_value, list):
        ids = []
        for item in cell_value:
            if isinstance(item, dict) and item.get("id"):
                cleaned = clean_id(item["id"])
                if cleaned:
                    ids.append(cleaned)
            elif isinstance(item, str):
                cleaned = clean_id(item)
                if cleaned:
                    ids.append(cleaned)
        return ids

    cell_str = str(cell_value).strip()
    if cell_str in ["", "[]"]:
        return []

    #JSON lists of strings
    if cell_str.startswith("["):
        parsed_data = safe_json_load(cell_str)
        if isinstance(parsed_data, list):
            ids = []
            for item in parsed_data:
                if isinstance(item, dict) and item.get("id"):
                    cleaned = clean_id(item["id"])
                    if cleaned:
                        ids.append(cleaned)
                elif isinstance(item, str):
                    cleaned = clean_id(item)
                    if cleaned:
                        ids.append(cleaned)
            return ids
        return []

    #Pipe-delimited or single strings
    raw_parts = cell_str.split("|")
    cleaned_ids = [clean_id(part) for part in raw_parts if part.strip()]
    return [cid for cid in cleaned_ids if cid]

def evaluate_federal_status(fid, ror_id, display_name, crossref_name):
    clean_fid = clean_id(fid) or ""
    clean_ror_str = clean_id(ror_id) or ""
    primary_name = str(display_name or "").strip()
    alt_name = str(crossref_name or "").strip()

    if clean_fid in US_FEDERAL_OPENALEX_IDS:
        return True, "Federal (OpenAlex ID Match)", US_FEDERAL_OPENALEX_IDS[clean_fid]
    if clean_ror_str in US_FEDERAL_ROR_IDS:
        return True, "Federal (ROR Match)", US_FEDERAL_ROR_IDS[clean_ror_str]
    if primary_name and US_FED_REGEX.search(primary_name):
        return True, "Federal (Keyword Match - OpenAlex Name)", "Matched via Regex"
    if alt_name and US_FED_REGEX.search(alt_name):
        return True, "Federal (Keyword Match - Crossref Name)", "Matched via Regex"
    return False, "Non-Federal/Foreign/Private/Unknown", "N/A"

def is_funder_id(value):
    cleaned = clean_id(value)
    if not cleaned:
        return False
    return cleaned.upper().startswith("F")

#Define API functions
def fetch_openalex_award(award_id):
    clean_aid = clean_id(award_id)
    if not clean_aid:
        return None
    if clean_aid in award_cache:
        return award_cache[clean_aid]
    try:
        url = f"https://api.openalex.org/awards/{clean_aid}"
        r = requests.get(url, headers={"User-Agent": f"mailto:{MAILTO}"})
        data = r.json() if r.status_code == 200 else None
        award_cache[clean_aid] = data
        time.sleep(DELAY)
        return data
    except Exception:
        award_cache[clean_aid] = None
        return None

def fetch_openalex_funder(fid):
    clean_fid = clean_id(fid)
    if not clean_fid:
        return None
    if clean_fid in openalex_cache:
        return openalex_cache[clean_fid]
    try:
        funder = Funders()[clean_fid]
        openalex_cache[clean_fid] = funder
        time.sleep(DELAY)
        return funder
    except Exception:
        openalex_cache[clean_fid] = None
        return None

def fetch_ror(ror_id):
    if not ror_id:
        return None
    if ror_id in ror_cache:
        return ror_cache[ror_id]
    try:
        url = f"https://api.ror.org/v2/organizations/{ror_id}"
        r = requests.get(url)
        data = r.json() if r.status_code == 200 else None
        ror_cache[ror_id] = data
        time.sleep(DELAY)
        return data
    except Exception:
        ror_cache[ror_id] = None
        return None

def fetch_crossref(doi):
    if not doi:
        return None
    if doi in crossref_cache:
        return crossref_cache[doi]
    try:
        url = f"https://data.crossref.org/fundingdata/funder/{doi}"
        r = requests.get(url)
        data = r.json() if r.status_code == 200 else None
        crossref_cache[doi] = data
        time.sleep(DELAY)
        return data
    except Exception:
        crossref_cache[doi] = None
        return None
#Identify unique funders
df_input = pd.read_excel(INPUT_FILE)
print(f"Loaded {len(df_input):,} rows from {INPUT_FILE}.")

all_funders = set()
award_work_links = []

for _, row in df_input.iterrows():
    funder_ids = extract_openalex_ids(row.get("funders"))
    for fid in funder_ids:
        if is_funder_id(fid):
            all_funders.add(fid)

    raw_awards = safe_json_load(row.get("awards"))
    if isinstance(raw_awards, list):
        for a in raw_awards:
            if isinstance(a, dict):
                aid = clean_id(a.get("id"))
                fid = clean_id(a.get("funder_id") or a.get("funder"))
                award_num = a.get("funder_award_id") or a.get("award_id")
            else:
                aid = clean_id(a)
                fid = None
                award_num = None

            if fid and is_funder_id(fid):
                all_funders.add(fid)

            award_work_links.append({
                "Institution": row.get("Institution"),
                "Work_ID": row.get("id"),
                "Work_DOI": row.get("doi"),
                "Work_Title": row.get("title"),
                "Publication_Year": row.get("publication_year"),
                "Award_OpenAlex_ID": aid,
                "Funder_ID": fid,
                "Raw_Award_Number": award_num
            })

print(f"{len(all_funders):,} unique funders and {len(award_work_links):,} grants found.")

#Get funder metadata
funder_metadata = {}

for fid in tqdm(all_funders, desc="Fetching funder metadata."):
    data = fetch_openalex_funder(fid)

    if not data:
        display_name, alt_titles, country_code, description, homepage_url = "", "", "", "", ""
        ror_id, doi = None, None
    else:
        display_name = data.get("display_name") or ""
        alt_titles = ",".join(data.get("alternate_titles", [])) if data.get("alternate_titles") else ""
        country_code = data.get("country_code") or ""
        description = data.get("description") or ""
        homepage_url = data.get("homepage_url") or ""
        ror_id = clean_ror(data.get("ids", {}).get("ror"))
        doi = clean_doi(data.get("ids", {}).get("doi"))

    #ROR data
    ror_types, ror_country, ror_lat, ror_lng, ror_city, ror_location = "", "", "", "", "", ""
    if ror_id:
        try:
            ror_data = fetch_ror(ror_id)
            if ror_data and isinstance(ror_data, dict):
                raw_types = ror_data.get("types", [])
                types_list = [t.get("type", "") if isinstance(t, dict) else str(t) for t in raw_types]
                ror_types = ",".join(filter(None, types_list))

                locations = ror_data.get("locations", [])
                if locations and isinstance(locations, list):
                    loc = locations[0] if isinstance(locations[0], dict) else {}
                    geo = loc.get("geonames_details", {}) if isinstance(loc.get("geonames_details"), dict) else {}
                    ror_country = geo.get("country_code", "") or geo.get("country_name", "") or loc.get("country_code", "")
                    ror_lat = geo.get("lat", "") or loc.get("lat", "")
                    ror_lng = geo.get("lng", "") or loc.get("lng", "")
                    ror_city = geo.get("name", "") or loc.get("name", "")
                    geonames_id = loc.get("geonames_id")
                    if geonames_id:
                        ror_location = f"https://www.geonames.org/{geonames_id}"
                    elif loc.get("url"):
                        ror_location = loc.get("url")
        except Exception as e:
            print(f"Warning: Failed to parse ROR data for ID {ror_id}: {e}")

    #Crossref data
    cross_country, cross_name, cross_type, cross_subtype, cross_region = "", "", "", "", ""
    cross_state_uri, cross_state_name, cross_state_country = "", "", ""

    if doi:
        try:
            cr = fetch_crossref(doi)
            if cr and isinstance(cr, dict):
                cross_country = cr.get("address", {}).get("postalAddress", {}).get("addressCountry", "")
                cross_name = cr.get("prefLabel", {}).get("Label", {}).get("literalForm", {}).get("content", "")
                cross_type = cr.get("fundingBodyType", "")
                cross_subtype = cr.get("fundingBodySubType", "")
                cross_region = cr.get("region", "")
                state_url = cr.get("state", {}).get("resource")
                cross_state_uri = state_url or ""

                #Geonames data
                if state_url:
                    if state_url in geonames_cache:
                        geo = geonames_cache[state_url]
                    else:
                        geonames_id = clean_id(state_url)
                        try:
                            geo_req = requests.get(
                                f"http://api.geonames.org/getJSON?geonameId={geonames_id}&username={GEONAMES_USERNAME}"
                            )
                            geo = geo_req.json() if geo_req.status_code == 200 and isinstance(geo_req.json(), dict) else {}
                        except Exception:
                            geo = {}
                        geonames_cache[state_url] = geo

                    cross_state_name = geo.get("name", "")
                    cross_state_country = geo.get("countryCode", "")
        except Exception as e:
            print(f"Warning: Failed to parse Crossref data for DOI {doi}: {e}")

    #Federal status review
    is_fed, fed_type, fed_agency = evaluate_federal_status(
        fid=fid, 
        ror_id=ror_id, 
        display_name=display_name, 
        crossref_name=cross_name
    )

    funder_metadata[fid] = {
        "OpenAlex_DisplayName": display_name,
        "OpenAlex_AlternateTitles": alt_titles,
        "OpenAlex_CountryCode": country_code,
        "OpenAlex_Description": description,
        "OpenAlex_HomepageURL": homepage_url,
        "OpenAlex_ROR": ror_id or "",
        "FunderDOI": doi or "",
        "Is_US_Federal_Funded": is_fed,
        "Federal_Funding_Flag_Type": fed_type,
        "Federal_Agency_Matched": fed_agency,
        "ROR_Types": ror_types,
        "ROR_Locations": ror_location,
        "ROR_CountryCode": ror_country,
        "ROR_Lat": ror_lat,
        "ROR_Long": ror_lng,
        "ROR_City": ror_city,
        "Crossref_Country": cross_country,
        "Crossref_Name": cross_name,
        "Crossref_Type": cross_type,
        "Crossref_Subtype": cross_subtype,
        "Crossref_Region": cross_region,
        "Crossref_StateURI": cross_state_uri,
        "Crossref_StateName": cross_state_name,
        "Crossref_StateCountry": cross_state_country,
    }

#Unnest rows and compile final dataframe
results = []

for row in tqdm(df_input.itertuples(), total=len(df_input), desc="Unnesting funder data"):
    funders = extract_openalex_ids(getattr(row, "funders", None))
    awards = extract_openalex_ids(getattr(row, "awards", None))
    all_row_funders = [fid for fid in set(funders + awards) if is_funder_id(fid)]

    for fid in all_row_funders:
        funder_data = funder_metadata.get(fid)
        if not funder_data:
            continue

        output = {
            "Institution": getattr(row, "Institution", None),
            "id": getattr(row, "id", None),
            "doi": getattr(row, "doi", None),
            "title": getattr(row, "title", None),
            "publication_year": getattr(row, "publication_year", None),
            "funderID": fid,
        }
        output.update(funder_data)
        results.append(output)

df_funders = pd.DataFrame(results)

#Get award/grant metadata
grant_rows = []
for item in tqdm(award_work_links, desc="Fetching award/grant metadata."):
    aid = item["Award_OpenAlex_ID"]
    award_data = fetch_openalex_award(aid) if aid else None

    row_data = {
        "Institution": item["Institution"],
        "Work_ID": item["Work_ID"],
        "Work_DOI": item["Work_DOI"],
        "Work_Title": item["Work_Title"],
        "Publication_Year": item["Publication_Year"],
        "Funder_ID": item["Funder_ID"],
        "Award_OpenAlex_ID": aid or "",
        "Funder_Award_Number": award_data.get("funder_award_id") if award_data else item["Raw_Award_Number"],
        "Award_Title": award_data.get("display_name") if award_data else "",
        "Award_Amount": award_data.get("amount") if award_data else None,
        "Award_Currency": award_data.get("currency") if award_data else "",
        "Start_Year": award_data.get("start_year") if award_data else None,
        "End_Year": award_data.get("end_year") if award_data else None,
        "Funding_Type": award_data.get("funding_type") if award_data else "",
        "Funded_Outputs_Count": award_data.get("funded_outputs_count") if award_data else None,
    }
    grant_rows.append(row_data)

df_grants = pd.DataFrame(grant_rows)

#Change URLs to text strings
url_columns = [
    "id", "funderID", "OpenAlex_HomepageURL", "OpenAlex_ROR", 
    "FunderDOI", "ROR_Locations", "Crossref_StateURI"
]

for col in url_columns:
    if col in df_funders.columns:
        df_funders[col] = df_funders[col].apply(
            lambda x: f"'{str(x)}" if pd.notna(x) and str(x) != "" else ""
        )

#Export data
with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
    df_funders.to_excel(writer, sheet_name="Funders", index=False)
    df_grants.to_excel(writer, sheet_name="Grants_and_Awards", index=False)

fed_count = df_funders["Is_US_Federal_Funded"].sum() if "Is_US_Federal_Funded" in df_funders.columns else 0
print(f"\nDone! File exported to {OUTPUT_FILE}. {fed_count:,} US federal rows, {len(df_funders):,} total rows.")
