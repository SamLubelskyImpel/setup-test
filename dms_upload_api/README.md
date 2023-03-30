# DMS Upload API

# Run locally
source venv/bin/activate
pip install -r requirements.txt
AWS_PROFILE=test python -m flask run

# Sample requests
curl -X 'POST' \
  'http://127.0.0.1:5000/fi-closed-deal/v1' \
  -H 'accept: application/json' \
  -H 'client_id: test_client' \
  -H 'x_api_key: test_api_key' \
  -H 'filename: my_file_name' \
  -H 'Content-Type: application/gzip' \
  --data-binary '@testfile.txt.gz'

curl -X 'POST' \
  'https://dms.testenv.impel.io/repair-order/v1' \
  -H 'accept: application/json' \
  -H 'client_id: test_client' \
  -H 'x_api_key: test_api_key' \
  -H 'filename: my_file_name' \
  -H 'Content-Type: application/gzip' \
  --data-binary '@testfile.txt.gz'


