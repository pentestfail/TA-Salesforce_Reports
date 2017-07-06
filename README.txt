# TA-Salesforce_Reports

Provides method to consume Salesforce.com reports via REST API without SOQL/SOSL queries to be indexed, put in kvstore, or both.  Take your existing reports or build new ones via the Salesforce.com UI and automate their ingestion into Splunk without the fuss of developer consoles or complex salesforce query languages.

This add-on provides a modular input to connect to Salesforce.com via user credentials and "security token" with options to index reports to a configurable index or store the responses to Splunk's kvstore, or both.  If storing reports to the kvstore, the input includes configuration options to:
- Create and update a kvstore (knowledge object) and specify its name
- Create and update a lookup (knowledge object) and specify its name
- Define a "key fieldname" from report to be used as the "_key" record for the kvstore (one or more comma delimited fields may be specified to define a unique record)
- Purge the kvstore at each update (deletes all records in the kvstore at each update)

## Requirements:
- Requires Salesforce security token (does not support 2-factor tokens). See Salesforce documentation on "resetting my security token" or options under "My Profile" in your account. (https://help.salesforce.com/articleView?id=user_security_token.htm)
- INFO logging provides detail to identify issues with authentication and report access but can be set to DEBUG for introspection into event level issues.

> More detailed documentation will be included in future updates.

## Submit issues or requests via Github:
TA-Salesforce_Reports: https://github.com/pentestfail/TA-Salesforce_Reports

### v1.0.4 (I hate API limits!)
- Changed Salesforce python client library to "Simple Salesforce"
- Changed from Salesforce reporting API to as source in favor of CSV export to overcome 2k record limit
- Minor UI enhancement to show more than 10 inputs per-page
- Improved status logging to checkpointer kvstore (future use)
- Improvements to lookup & kvstore field creation & update methods (issue with lookups not updating for new fields)
- Additional "purge" options to allow for multiple reports writing to same kvstore
  > **"All Records":** purge all contents of the specified kvstore (default)  
   **"Report Only":** purge records created by this input (multiple inputs may write to same kvstore)  
   **"Disabled":** disable purge for the input (may result in duplicate records)
- The "KVStore Key Fieldname" parameter will accept comma-separated field values (enclose in quotes if spaces in fieldnames) and concatenate with a hyphen to create unique row identifiers when individual fields won't
- Added "_updated" and "_input_id" meta fields to kvstore functions (not added to lookups or kvstore fields; may be manually added)
  > **"_updated":** timestamp of report row insertion to assist with troubleshooting  
  > **"_input_id":** enables identification of input which inserted record into kvstore (used in purge functions)


	**NOTICE:** this release assumes default Salesforce CSV export behavior (appends a footer to the CSV) & drops the last 5 lines of the report.  If your Salesforce instance does not append the footer, your report may not be complete! A parameter will be added in next release.

### v1.0.2 (Initial Release)