# TA-Salesforce_Reports

Provides method to consume Salesforce.com reports via REST API without SOQL/SOSL queries to be indexed, put in kvstore, or both.  Take your existing reports or build new ones via the Salesforce.com UI and automate their ingestion into Splunk without the fuss of developer consoles or complex salesforce query languages.

This add-on provides a modular input to connect to Salesforce.com via user credentials and "security token" with options to index reports to a configurable index or store the responses to Splunk's kvstore, or both.  If storing reports to the kvstore, the input includes configuration options to:
- Create and update a kvstore (knowledge object) and specify its name
- Create and update a lookup (knowledge object) and specify its name
- Define a "key fieldname" from report to be used as the "_key" record for the kvstore (one or more comma delimited fields may be specified to define a unique record)
- Purge the kvstore at each update (deletes all records in the kvstore at each update)

## Requirements:
- Requires Salesforce security token (does not support 2-factor tokens). See Salesforce documentation on "resetting my security token" or options under "My Profile" in your account. (https://help.salesforce.com/articleView?id=user_security_token.htm)
- Due to limitations in the Salesforce REST API, reports may be limited to 2,000 records.  It is suggested to use time based queries to limit the number of records returned.  It is possible to create multiple reports to retrieve all data if filtered systematically.
- INFO logging provides detail to identify issues with authentication and report access but can be set to DEBUG for introspection into event level issues.

> More detailed documentation will be included in future updates.

## Submit issues or requests via Github:
TA-Salesforce_Reports: https://github.com/pentestfail/TA-Salesforce_Reports