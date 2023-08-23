# Kawasaki integrations
Parse XML files from web providers to upload to ICC ftp server.

### Onboarding a new dealer
- Add the dealer's web_url, web_suffix, and impel_id to kawasaki_config.json
-
- dealerspike by default appends web_suffix /feeds.asp?feed=GenericXMLFeed&version=2 to web_url
- ari by default appends web_suffix /unitinventory_univ.xml to web_url
- dx1 by default appends no web_suffix to web_url
- Specify web_suffix in kawasaki_config.json to override the default web_suffix
- 
- run: cd tests;python test_kawasaki.py
- Verify the final log message shows all configs work
- Check the generated csv file in tests/output for the newly added config and verify data appears and is properly formatted csv
- Copy the contents of kawasaki_config.json into https://jsonformatter.curiousconcept.com/# and validate it is valid JSON
- Upload kawasaki_config.json to the kawasaki-us-east-1-test bucket
- Upload kawasaki_config.json to the kawasaki-us-east-1-prod bucket
