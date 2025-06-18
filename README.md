# Scholarly Output Analysis

A collection of processes used to harvest data from the OpenAlex API and potentially other sources in order to analyze institutional scholarly output. More will be added as they are developed and refined.

**Note:** Opening exported workbooks may bring up an error message of "We found a problem with some content in 'file.xlsx'. Do you want us to try to recover as much as we can? If you trust the source of this workbook, click Yes.". This is due to the links present in the output data. Opening the workbook, converting everything to text, and saving should fix this.

### Open Alex Output Brief
This script is used to generate a brief listing of output by calendar year and institution, limiting returned fields to corresponding_institution_ids, display_name, doi, and title. It can be combined with publication lists from other resources to then use with openalex_dois.py, or can be used as a starting point to understand basic institutional output and corresponding authorship. Fields can be added as needed, but the script does not include steps to flatten nested fields.

### Open Alex Output Full
This script is used to generate a detailed listing of output by calendar year and institution and flattens the nested apc_list, apc_paid, open_access, primary_location, and primary_topic fields into individual fields based on identified needs. Additional fields can be flattened as needed by following the same dataframe pattern.

### Open Alex DOIs
This script is meant to be used in conjunction with a pre-existing file listing DOIs of publications from Scopus, Web of Science, or other resource, potentially combined with the output generated from openalex_output_brief.py. After removing duplicate entries from the list, the DOIs are then used to query OpenAlex to harvest additional publication details. The output is modeled after openalex_output_full.py.

### Open Alex Cited Works
This script is meant to be used with a pre-existing file listing OpenAlex Work IDs obtained by openalex_output_brief.py, openalex_dois.py, or other methods. The Work IDs are used to query OpenAlex to harvest publication details for each work cited by the individual work.
