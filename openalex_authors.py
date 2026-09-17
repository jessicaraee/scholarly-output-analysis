#Use case: Use data exported from openalex_output.py to query OpenAlex to harvest additional author metadata in an unnested, row-by-row format and apply text matching to assign organizational entities.

#Import libraries
import pandas as pd
import json
import re
import time
import requests
import pyalex
from pyalex import Institutions
from tqdm import tqdm
from rapidfuzz import process, fuzz

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
OUTPUT_FILE = f"/filepath/filename_authors.xlsx"

#Dictionaries and lookup tables
AFFILIATE_LOOKUP = {
  "institution_openalexid1": "institution_rorid1", #Replace with OpenAlex and ROR IDs of all institutions to look up
  "institution_openalexid2": "institution_rorid2",
  "institution_openalexid3": "institution_rorid3"
}

ABBREVIATION_MAP = {
    #General terms
    r"\bDept\.?\b": "Department",
    r"\bDiv\.?\b": "Division",
    r"\bInst\.?\b": "Institute",
    r"\bCtr\.?\b": "Center",
    r"\bSch\.?\b": "School",
    r"\bLab\.?\b": "Laboratory",
    
    #School abbreviations
    r"\bSEAS\b": "School of Engineering and Applied Sciences",
    r"\bGSAS\b": "Graduate School of Arts and Sciences",
    r"\bGSA\b": "Graduate School of Architecture",
}

DEPARTMENT_PATTERNS = [
    r"(Department of [^,;]+)",
    r"(Division of [^,;]+)",
    r"(School of [^,;]+)",
    r"(Faculty of [^,;]+)",
    r"(College of [^,;]+)",
    r"(Institute of [^,;]+)",
    r"(Center for [^,;]+)",
    r"(Centre for [^,;]+)",
    r"(Program in [^,;]+)",
    r"(Unit of [^,;]+)",
]

INSTITUTION_SCHOOLS = [
    "School1", "School2", "School3"
] #List of institution schools

INSTITUTION_DEPARTMENTS = [
    "Department1", "Department2", "Department3", "Applied Physics", "Applied Mathematics"
] #List of institution departments

INSTITUTION_INSTITUTES = [
    "Institute1", "Institute2", "Institute3"
] #List of institution affiliated institutes

ALL_TARGET_ENTITIES = sorted(list(set(INSTITUTION_SCHOOLS + INSTITUTION_DEPARTMENTS + INSTITUTION_INSTITUTES)))

school_pattern = re.compile(r"\b(" + "|".join([re.escape(s) for s in INSTITUTION_SCHOOLS]) + r")\b", re.IGNORECASE)
dept_pattern = re.compile(r"\b(" + "|".join([re.escape(d) for d in INSTITUTION_DEPARTMENTS]) + r")\b", re.IGNORECASE)
inst_pattern = re.compile(r"\b(" + "|".join([re.escape(i) for i in INSTITUTION_INSTITUTES]) + r")\b", re.IGNORECASE)

#Define helper functions
def safe_json_load(value):
    if pd.isna(value):
        return []
    try:
        return json.loads(value) if isinstance(value, str) else value
    except Exception:
        return []

def safe_url_text(url):
    if pd.isna(url) or not url:
        return ""
    url = str(url)
    url = re.sub(r"^https?://", "", url)
    return url

def clean_id(openalex_url):
    if pd.isna(openalex_url) or not openalex_url:
        return None
    return str(openalex_url).split("/")[-1]

#Text processing/matching functions
def expand_abbreviations(text):
    if not text or pd.isna(text):
        return text
    expanded = str(text)
    for pattern, replacement in ABBREVIATION_MAP.items():
        expanded = re.sub(pattern, replacement, expanded, flags=re.IGNORECASE)
    return expanded

def extract_all_departments(text):
    if pd.isna(text) or not text:
        return None
    text = str(text).strip()
    all_matches = []
    for pattern in DEPARTMENT_PATTERNS:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        for m in matches:
            extracted = re.sub(r"\s+", " ", m.strip())
            all_matches.append(extracted)
    return "; ".join(all_matches) if all_matches else None

def strip_department_keyword(dept_text):
    if not dept_text:
        return None
    keywords = [
        "Department of", "Division of", "School of", "Faculty of",
        "College of", "Institute of", "Center for", "Centre for",
        "Program in", "Unit of",
    ]
    parts = [p.strip() for p in dept_text.split(";")]
    stripped_parts = []
    for p in parts:
        stripped = p
        for kw in keywords:
            stripped = re.sub(rf"^{kw}\s+", "", stripped, flags=re.IGNORECASE)
        stripped_parts.append(stripped)
    return "; ".join(stripped_parts)

def fuzzy_match_department(text, threshold=85):
    if not text or pd.isna(text):
        return None, 0
    best_match = process.extractOne(
        text, 
        ALL_TARGET_ENTITIES, 
        scorer=fuzz.partial_ratio
    )
    if best_match and best_match[1] >= threshold:
        return best_match[0], round(best_match[1], 2)
    return None, 0

def extract_matched_school(affiliation_str):
    if not affiliation_str:
        return None
    match = school_pattern.search(affiliation_str)
    return match.group(0) if match else None

def extract_matched_department(affiliation_str):
    if not affiliation_str:
        return None
    match = dept_pattern.search(affiliation_str)
    return match.group(0) if match else None

def extract_matched_institute(affiliation_str):
    if not affiliation_str:
        return None
    match = inst_pattern.search(affiliation_str)
    return match.group(0) if match else None

def tag_affiliation(inst_id, ror_id, parent_insts, raw_affil_str):
    clean_inst_id = clean_id(inst_id)
    clean_ror = safe_url_text(ror_id)
    raw_text = str(raw_affil_str or "").lower()

    if clean_inst_id in AFFILIATE_LOOKUP:
        return True, "OpenAlex_ID"

    for eid, ror in AFFILIATE_LOOKUP.items():
        if ror and ror in clean_ror:
            return True, "ROR_ID"

    if parent_insts:
        parents_lower = str(parent_insts).lower()
        if any(k in parents_lower for k in ["affiliate1", "affiliate2", "affiliate3", "institution"]):
            return True, "Parent_Tree"

    if any(k in raw_text for k in ["institution", "affiliate1", "affiliate3", "affiliate3Abbreviated", "affiliate4", "affiliate4Abbreviated", "affiliate4OtherName"]):
        return True, "Text_Keyword"

    return False, "Unmatched"

def derive_campus(raw_text=""):
    t = str(raw_text).lower()
    if any(k in t for k in ["affiliate4", "affiliate4Abbreviated", "affiliate4OtherName"]):
        return "affiliate4Formatted"
    if "affiliate1" in t:
        return "affiliate1Formatted"
    if "affiliate2" in t:
        return "affiliate2Formatted"
    if any(k in t for k in ["affiliate3", "affiliate3Abbreviated", "affiliate3OtherName"]):
        return "affiliate3Formatted"
    return "institutionFormatted"

#API functions
inst_cache = {}

def prefetch_institution_metadata(institution_ids):
    unique_ids = list({clean_id(i) for i in institution_ids if clean_id(i)})
    missing_ids = [i for i in unique_ids if i not in inst_cache]

    if not missing_ids:
        return

    print(f"Prefetching metadata for {len(missing_ids)} unique institution IDs...")
    batch_size = 50

    for i in range(0, len(missing_ids), batch_size):
        chunk = missing_ids[i : i + batch_size]
        try:
            results = Institutions().filter(openalex_id="|".join(chunk)).get(per_page=50)

            for inst in results:
                cid = clean_id(inst.get("id"))
                parents = inst.get("associated_institutions", [])
                parent_names = [
                    p.get("display_name")
                    for p in parents
                    if isinstance(p, dict) and p.get("relationship") == "parent"
                ]

                inst_cache[cid] = {
                    "InstitutionName": inst.get("display_name"),
                    "ROR_ID": safe_url_text(inst.get("ror")),
                    "CountryCode": inst.get("country_code"),
                    "InstitutionType": inst.get("type"),
                    "ParentInstitutions": "; ".join(parent_names) if parent_names else None,
                }
        except Exception as e:
            print(f"Warning: Failed to fetch metadata batch: {e}")

    for cid in missing_ids:
        if cid not in inst_cache:
            inst_cache[cid] = {
                "InstitutionName": None, "ROR_ID": None, 
                "CountryCode": None, "InstitutionType": None, "ParentInstitutions": None,
            }

#Load and parse data
df = pd.read_excel(INPUT_FILE)
print(f"{len(df)} works loaded.")

df["authorships_parsed"] = df["authorships"].apply(safe_json_load)

#Collect all unique Institution IDs for batch querying
all_inst_ids = []
for authorships in df["authorships_parsed"]:
    for authorship in authorships:
        affiliations_list = authorship.get("affiliations", [])
        for aff_item in affiliations_list:
            inst_obj = aff_item.get("institution")
            if isinstance(inst_obj, dict) and inst_obj.get("id"):
                all_inst_ids.append(inst_obj.get("id"))
            else:
                row_inst_ids = aff_item.get("institution_ids", [])
                all_inst_ids.extend(row_inst_ids)

prefetch_institution_metadata(all_inst_ids)

#Extract author rows
author_rows = []

for _, row in tqdm(df.iterrows(), total=len(df)):
    work_id = row.get("id")
    work_doi = row.get("doi")
    title = row.get("display_name")
    publication_year = row.get("publication_year")

    parsed_authorships = row.get("authorships_parsed", [])

    for authorship in parsed_authorships:
        author = authorship.get("author", {})
        author_id = clean_id(author.get("id"))
        author_pos = authorship.get("author_position")
        is_corresponding = authorship.get("is_corresponding")
        if is_corresponding is True:
            corresponding_status = "Yes"
            corresponding_type = "Explicit (OpenAlex Marked)"
        elif author_pos == "first":
            corresponding_status = "Possibly"
            corresponding_type = "Inferred (First Author)"
        elif author_pos == "last":
            corresponding_status = "No"
            corresponding_type = "Inferred (Last Author)"
        else:
            corresponding_status = "No"
            corresponding_type = "Inferred (Middle Author)"

        affiliations_list = authorship.get("affiliations", [])

        if not affiliations_list:
            raw_affs = authorship.get("raw_affiliation_strings", [])
            affiliations_list = (
                [{"raw_affiliation_string": aff, "institution_ids": [], "institution": {}} for aff in raw_affs]
                if raw_affs
                else [{"raw_affiliation_string": None, "institution_ids": [], "institution": {}}]
            )

        for aff_item in affiliations_list:
            raw_affiliation = aff_item.get("raw_affiliation_string")
            expanded_affiliation = expand_abbreviations(raw_affiliation)

            raw_inst_id = None
            inst_name_from_json = None

            inst_obj = aff_item.get("institution")
            if isinstance(inst_obj, dict):
                raw_inst_id = inst_obj.get("id")
                inst_name_from_json = inst_obj.get("display_name")

            if not raw_inst_id:
                row_institution_ids = aff_item.get("institution_ids", [])
                raw_inst_id = row_institution_ids[0] if row_institution_ids else None

            institution_id = clean_id(raw_inst_id)

            meta = inst_cache.get(institution_id, {})
            institution_name_matched = inst_name_from_json or meta.get("InstitutionName")

            is_institution, match_method = tag_affiliation(
                inst_id=institution_id,
                ror_id=meta.get("ROR_ID"),
                parent_insts=meta.get("ParentInstitutions"),
                raw_affil_str=expanded_affiliation,
            )

            campus_name = derive_campus(expanded_affiliation) if is_institution else "Non-Institution"

            departments_full = extract_all_departments(expanded_affiliation)
            departments_stripped = (
                strip_department_keyword(departments_full)
                if departments_full
                else None
            )

            matched_school = extract_matched_school(expanded_affiliation)
            matched_dept = extract_matched_department(expanded_affiliation)
            matched_inst = extract_matched_institute(expanded_affiliation)

            fuzzy_dept, fuzzy_score = None, 0
            if is_institution and not matched_dept and expanded_affiliation:
                fuzzy_dept, fuzzy_score = fuzzy_match_department(expanded_affiliation, threshold=85)

            best_department = matched_dept or fuzzy_dept or departments_stripped

            author_rows.append({
                "OpenAlex_Work_ID": work_id,
                "Work_DOI": safe_url_text(work_doi),
                "Title": title,
                "Publication_Year": publication_year,
                "Author_Name": author.get("display_name"),
                "Author_OpenAlex_ID": author_id,
                "Author_Position": author_pos,
                "Is_Corresponding_Author": corresponding_status,
                "Corresponding_Flag_Type": corresponding_type,
                "Author_ORCID": safe_url_text(author.get("orcid")),
                "Is_Affiliate": is_institution,
                "Campus": campus_name,
                "Match_Method": match_method,
                "Matched_School": matched_school,
                "Matched_Department": best_department,
                "Institute_or_Program": matched_inst,
                "Fuzzy_Match_Score": fuzzy_score if fuzzy_dept else None,
                "Institution_OpenAlex_ID": institution_id,
                "Institution_Display_Name": institution_name_matched,
                "Institution_ROR_ID": meta.get("ROR_ID"),
                "Institution_Country_Code": meta.get("CountryCode"),
                "Institution_Type": meta.get("InstitutionType"),
                "Parent_Institutions": meta.get("ParentInstitutions"),
                "Raw_Affiliation_String": raw_affiliation,
                "Raw_Pulled_Department_Text": departments_full,
            })

#Create QA tables
authors_df = pd.DataFrame(author_rows)

institution_authors_df = authors_df[authors_df["Is_Affiliate"] == True].copy()

campus_summary = (
    institution_authors_df.groupby(["Campus", "Matched_School", "Matched_Department"], dropna=False)
    .agg(
        Total_Authorships=("OpenAlex_Work_ID", "count"),
        Unique_Works=("OpenAlex_Work_ID", "nunique"),
        Unique_Authors=("Author_OpenAlex_ID", "nunique"),
        Corresponding_Authorships=("Is_Corresponding_Author", lambda x: (x == "Yes").sum()),
        Possible_Corresponding_Authorships=("Is_Corresponding_Author", lambda x: (x == "Possibly").sum())
    )
    .reset_index()
    .sort_values(by=["Total_Authorships", "Unique_Works"], ascending=False)
)

corresponding_authors_df = authors_df[
    (authors_df["Is_Corresponding_Author"].isin(["Yes", "Possibly"])) & 
    (authors_df["Is_Affiliate"] == True)
].copy()

external_collaborators = (
    authors_df[authors_df["Is_Affiliate"] == False]
    .groupby(["Institution_Display_Name", "Institution_Country_Code", "Institution_OpenAlex_ID", "Institution_ROR_ID", "Institution_Type"], dropna=False)
    .agg(
        CoAuthorship_Count=("OpenAlex_Work_ID", "count"),
        Unique_Works=("OpenAlex_Work_ID", "nunique"),
        Unique_External_Authors=("Author_OpenAlex_ID", "nunique")
    )
    .reset_index()
    .sort_values(by="CoAuthorship_Count", ascending=False)
)

total_records = len(institution_authors_df)
match_summary = (
    institution_authors_df["Match_Method"]
    .value_counts()
    .reset_index()
)
match_summary.columns = ["Match_Method", "Count"]
match_summary["Percentage"] = (match_summary["Count"] / total_records * 100).round(2) if total_records > 0 else 0

actionable_qa = institution_authors_df[
    (institution_authors_df["Matched_Department"].isna()) | 
    (institution_authors_df["Matched_Department"] == "") |
    (institution_authors_df["Author_ORCID"].isna()) |
    (institution_authors_df["Author_ORCID"] == "")
][[
    "OpenAlex_Work_ID", 
    "Work_DOI", 
    "Author_Name", 
    "Author_ORCID", 
    "Raw_Affiliation_String", 
    "Campus", 
    "Matched_School", 
    "Matched_Department"
]].copy()

actionable_qa["Issue_Type"] = actionable_qa.apply(
    lambda r: "Missing Department & ORCID" if (pd.isna(r["Matched_Department"]) or r["Matched_Department"] == "") and (pd.isna(r["Author_ORCID"]) or r["Author_ORCID"] == "")
    else ("Missing Department Tag" if (pd.isna(r["Matched_Department"]) or r["Matched_Department"] == "") else "Missing ORCID"),
    axis=1
)

#Clean URLs and export to Excel
url_columns = ["Work_DOI", "Author_OpenAlex_ID", "Author_ORCID", "Institution_OpenAlex_ID", "Institution_ROR_ID"]
for df_target in [authors_df, institution_authors_df, corresponding_authors_df, actionable_qa]:
    for col in url_columns:
        if col in df_target.columns:
            df_target[col] = df_target[col].apply(lambda x: f"'{str(x)}" if pd.notna(x) and str(x) != "" else "")

with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
    authors_df.to_excel(writer, sheet_name="All_Authorships", index=False)
    institution_authors_df.to_excel(writer, sheet_name="Affiliates_Only", index=False)
    campus_summary.to_excel(writer, sheet_name="Campus_Summary", index=False)
    corresponding_authors_df.to_excel(writer, sheet_name="Corresponding_Authors", index=False)
    external_collaborators.to_excel(writer, sheet_name="External_Collaborators", index=False)
    match_summary.to_excel(writer, sheet_name="Match_Method_Summary", index=False)
    actionable_qa.to_excel(writer, sheet_name="Actionable_QA_Review", index=False)

print(f"Done! File exported to {OUTPUT_FILE}.")
