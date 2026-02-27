# scholarly-output-analysis

A collection of processes used to harvest data from the OpenAlex API and potentially other sources in order to analyze institutional scholarly output. More will be added as they are developed and refined.

**Note:** Opening exported workbooks may bring up an error message of "We found a problem with some content in 'file.xlsx'. Do you want us to try to recover as much as we can? If you trust the source of this workbook, click Yes.". This is due to the links present in the output data. Opening the workbook, converting everything to text, and saving should fix this.

### openalex_output.py
This script is used to generate a detailed listing of output by calendar year and institution and flattens nested columns into individual fields. Multiple institutions can be searched in a single query. Output columns can be customized by editing the columns_to_keep list. 

### openalex_dois.py
This script is meant to be used in conjunction with a pre-existing file listing DOIs of publications from Scopus, Web of Science, or other resource, potentially combined with the output generated from openalex_output_brief.py. After removing duplicate entries from the list, the DOIs are then used to query OpenAlex to harvest additional publication details. The output is modeled after openalex_output_full.py. (Has not been updated for new API.)

### openalex_citedworks.py
This script is meant to be used with a pre-existing file listing OpenAlex Work IDs obtained by openalex_output.py, openalex_dois.py, or other methods. The Work IDs are used to query OpenAlex to harvest publication details for each work cited by the individual work. (Has not been updated for new API.)
