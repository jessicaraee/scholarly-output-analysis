#Use case: Use the pyalex wrapper to query the OpenAlex API to pull a detailed list of publications by CY and institution ID, customizing included fields as needed.
#Updated 7/8/2026 to add topic filter, type filter, grant filter toggle, and text search filter.

#Import libraries
import pandas as pd
import pyalex
from pyalex import Works
from tqdm import tqdm
from datetime import datetime, timedelta
import os
import csv
import json

#Configure query
calendar_year = 2025
institution_map = {
  "institution_id1": "institution_name1", #Replace with ID to query for and name of institution
  "institution_id2": "institution_name2",
  "institution_id3": "institution_name3"
}

institution_id = list(institution_map.keys())[0]

#Set any filter to None to pull everything
FILTER_OA_STATUS = None             #"bronze", "diamond", "gold", "green", "hybrid", "closed"
FILTER_IS_OA = None                 #True, False
FILTER_TYPE = "dataset"             #"article", "book", "book-chapter", "dataset", "editorial", "erratum", "letter", "paratext", "peer-review", "preprint", "reference-entry", "report", "review", "other"
FILTER_ISSN = None                  #ex. "0000-0000"
FILTER_DOMAIN = None                #"1" = Life Sciences, "2" = Social Sciences, "3" = Physical Sciences, "4" = Health Sciences
FILTER_FIELD = "2"                  #See OpenAlex_TaxonomyMatrix for field numbers
FILTER_SUBFIELD = None              #See OpenAlex_TaxonomyMatrix for subfield numbers
FILTER_TOPIC = None                 #See OpenAlex_TaxonomyMatrix for topic numbers
FILTER_GRANTS = False               #False = all results, True = only if awards field is not blank
FILTER_INSTITUTION_QUERY = True           #False = institution_id only, True = institution_id + text search

INSTITUTION_QUERY_STRING = (
  '("Institution Var1" OR "Institution Var2" OR "Institution Var3")'
  'NOT "Wrong Institution1" NOT "Wrong Institution2"'
)

API_KEY = "key"
MAILTO = "youremail@youremail.com"
if not MAILTO:
    raise ValueError('Email address needed for polite pool')

#Configure pyalex
pyalex.config.api_key = API_KEY
pyalex.config.email = MAILTO
pyalex.config.max_retries = 5

#Configure files
OUTPUT_CSV = f"/filepath/openalex_{institution_id}_CY{calendar_year}_output.csv"
OUTPUT_XLSX = OUTPUT_CSV.replace(".csv", ".xlsx")

#Select output columns
columns_to_keep = [
    "Match Type",
    "id",
    "doi",
    "title",
    "display_name",
    "publication_year",
    "publication_date",
    "ids",
    "language",
    "type",
    "indexed_in",
    "authorships",
    "corresponding_author_ids",
    "corresponding_institution_ids",
    "fwci",
    "cited_by_count",
    "citation_normalized_percentile",
    "cited_by_percentile_year",
    "biblio",
    "is_retracted",
    "is_paratext",
    "is_xpac",
    "topics",
    "keywords",
    "concepts",
    "mesh",
    "locations_count",
    "locations",
    "best_oa_location",
    "sustainable_development_goals",
    "awards",
    "funders",
    "has_content",
    "referenced_works_count",
    "referenced_works",
    "related_works",
    "counts_by_year",
    "updated_date",
    "created_date",
    "primary_location.is_oa",
    "primary_location.landing_page_url",
    "primary_location.source.display_name",
    "primary_location.source.issn_l",
    "primary_location.source.is_oa",
    "primary_location.source.is_in_doaj",
    "primary_location.source.host_organization_name",
    "primary_location.source.host_organization_lineage_names",
    "primary_location.source.type",
    "primary_location.is_accepted",
    "primary_location.is_published",
    "open_access.is_oa",
    "open_access.oa_status",
    "open_access.any_repository_has_fulltext",
    "apc_list.value_usd",
    "apc_paid.value_usd",
    "primary_topic.display_name",
    "primary_topic.subfield.display_name",
    "primary_topic.field.display_name",
    "primary_topic.domain.display_name"
]

FINAL_COLUMNS = ["Institution", "InstitutionID"] + columns_to_keep

#Flatten nested fields, clean work ID, and add identification columns
def flatten_results(results, inst_name=None, inst_id=None):
    for r in results:
        r["id"] = r["id"].split("/")[-1]
        if inst_name:
            r["Institution"] = inst_name
        if inst_id:
            r["InstitutionID"] = inst_id

    df = pd.json_normalize(results, sep=".")

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_COLUMNS]

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
            )
    return df

#Define quarters
quarters = [
    ("Q1", f"{calendar_year}-01-01", f"{calendar_year}-03-31"),
    ("Q2", f"{calendar_year}-04-01", f"{calendar_year}-06-30"),
    ("Q3", f"{calendar_year}-07-01", f"{calendar_year}-09-30"),
    ("Q4", f"{calendar_year}-10-01", f"{calendar_year}-12-31"),
]

if os.path.exists(OUTPUT_CSV):
    os.remove(OUTPUT_CSV)

first_write = True
total_harvested = 0

#Loop through quarterly harvest
for inst_id, inst_name in institution_map.items():
    for quarter_label, start_str, end_str in quarters:
        print(f"\nProcessing {inst_name} {quarter_label}: {start_str}→{end_str}")
        
        combined_works = {}

        api_filters = {
            "from_publication_date": start_str,
            "to_publication_date": end_str,
        }

        if FILTER_OA_STATUS:
            api_filters["open_access"] = {"oa_status": str(FILTER_OA_STATUS)}

        if FILTER_IS_OA is not None:
            if "open_access" not in api_filters:
                api_filters["open_access"] = {}
            api_filters["open_access"]["is_oa"] = FILTER_IS_OA

        if FILTER_TYPE:
            api_filters["type"] = str(FILTER_TYPE)

        if FILTER_ISSN:
            api_filters["primary_location"] = {"source": {"issn_l": str(FILTER_ISSN)}}

        if FILTER_DOMAIN:
            api_filters["primary_topic"] = {"domain": {"id": str(FILTER_DOMAIN)}}

        if FILTER_FIELD:
            api_filters["primary_topic"] = {"field": {"id": str(FILTER_FIELD)}}

        if FILTER_SUBFIELD:
            api_filters["primary_topic"] = {"subfield": {"id": str(FILTER_SUBFIELD)}}

        if FILTER_TOPIC:
            api_filters["primary_topic"] = {"id": str(FILTER_TOPIC)}

        #Run institution id query and filter based on parameters      
        id_query = Works().filter(
            authorships={"institutions": {"lineage": inst_id}},
            **api_filters
        )
        
        for page in id_query.paginate(per_page=100, n_max=None):
            for work in page:
                w_id = work["id"]
                combined_works[work["id"]] = {
                    "data": work,
                    "source": "Institution ID Query"
                }

        #Run backup text query
        if FILTER_INSTITUTION_QUERY:
            affiliation_filter = api_filters.copy()
            affiliation_filter["raw_affiliation_strings.search"] = INSTITUTION_QUERY_STRING
            
            text_query = Works().filter(**affiliation_filter)

            #Tag match type
            for page in text_query.paginate(per_page=100, n_max=None):
                for work in page:
                    w_id = work["id"]
                    if w_id in combined_works:
                        combined_works[w_id]["source"] = "Institution ID"
                    else:
                        combined_works[w_id] = {
                            "data": work,
                            "source": "Text Query Fallback"
                        }
        else:
            print("FILTER_INSTITUTION_QUERY turned off, searched by Institution ID only.")

        #Apply grants filter
        all_results = []
        for work_id, wrapper in combined_works.items():
            work_data = wrapper["data"]
            source_flag = wrapper["source"]
            
            if FILTER_GRANTS:
                grants = work_data.get("grants", []) or work_data.get("awards", [])
                if not grants:
                    continue 

            work_data["Match Source"] = source_flag
            all_results.append(work_data)

        count_found = len(all_results)
        print(f"Found {count_found} unique combined works for {quarter_label}.")

        if not all_results:
            continue

        #Flatten results and drop duplicates
        for r in all_results:
            r["Institution"] = inst_name
            r["InstitutionID"] = inst_id

        df_slice = flatten_results(all_results)
        df_slice = df_slice.drop_duplicates(subset="id")

        #Append results to temporary csv
        df_slice.to_csv(
            OUTPUT_CSV,
            mode="a",
            index=False,
            header=first_write,
            quoting=csv.QUOTE_ALL
        )
        first_write = False

        total_harvested += count_found
        print(f"{total_harvested} unique works harvested so far.")

print(f"\nHarvest complete! {total_harvested} total unique works found.")

#Change returned URLs to text, convert csv to xlsx, and export data
if os.path.exists(OUTPUT_CSV):
    df_full = pd.read_csv(OUTPUT_CSV)
    url_columns = ["doi", "primary_location.landing_page_url"]

    for col in url_columns:
        if col in df_full.columns:
            df_full[col] = df_full[col].apply(lambda x: f"'{str(x)}" if pd.notna(x) else "")

    df_full.to_excel(OUTPUT_XLSX, index=False)
    print(f"Done! File exported to {OUTPUT_XLSX}.")

    os.remove(OUTPUT_CSV)
    print(f"Temporary csv {OUTPUT_CSV} deleted.")
else:
    print("\nNo records matched the criteria so no output files were created.")
