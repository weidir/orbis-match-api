# Orbis Match bulk API Application

### Important files
- orbis_match_api_call.py: script to run that executes query againt the Orbis Match API
- src/utils.py: file containing "OrbisMatchAPIQueryClient" class with methods used in "orbis_match_api_call.py" script
- conf/config.py: file containing Orbis API credentials (expecting name "orbis_api_key")
### Steps to query the Orbis Match bulk API
1. In the "orbis_match_api_call.py", make sure the variables set within the 'if __name__ == "__main__"' block are set correctly (e.g., "path_to_data", "company_name_col", etc.).
2. Execute the "orbis_match_api_call.py" script.