# Kawasaki integrations
Parse XML files from web providers to upload to ICC ftp server.

### Onboarding a new dealer
- Add the dealer url to kawasaki_config.json
- run: cd tests;python test_kawasaki.py
- Verify the final log message shows all configs work, warning messages of unexpected tags are ok
- Check the generated csv file in tests/output for the newly added config and verify data appears and is properly formatted csv
- Copy the contents of kawasaki_config.json into https://jsonformatter.curiousconcept.com/# and validate it is valid JSON
- Upload kawasaki_config.json to the kawasaki-us-east-1-test bucket
- Upload kawasaki_config.json to the kawasaki-us-east-1-prod bucket
