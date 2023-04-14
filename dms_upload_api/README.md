# DMS Upload API

# Run locally
source venv/bin/activate
pip install -r requirements.txt
AWS_PROFILE=test python -m flask run

# Sample requests
curl -X 'POST' \
  'http://127.0.0.1:5000/dms-upload/v1' \
  -H 'accept: application/json' \
  -H 'client_id: test_client' \
  -H 'x_api_key: test_api_key' \
  -H 'filename: RRO_D_RO_XXXX_612841577413339__01_01_001_91772ef0-e758-4886-b070-4db4cafe5366.gz' \
  -H 'Content-Type: application/gzip' \
  --data-binary '@testfile.txt.gz'

curl -X 'POST' \
  'https://dms.testenv.impel.io/dms-upload/v1' \
  -H 'accept: application/json' \
  -H 'client_id: test_client' \
  -H 'x_api_key: test_api_key' \
  -H 'filename: RRO_D_DH_XXXX_612841577413339__01_01_001_91772ef0-e758-4886-b070-4db4cafe5366.gz' \
  -H 'Content-Type: application/gzip' \
  --data-binary '@testfile.txt.gz'


