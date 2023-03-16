import requests
url = "https://dms.testenv.impel.io/repair-order/v1"
headers = {
 "client_id": "test",
 "x_api_key": "test",
 "Content-Type": "application/json",
 "accept": "application/json"
}
json = {
  "sample_string": "12345ABC",
  "sample_obj": {
    "sample_string": "12345ABC"
  }
}
resp = requests.get(url, headers=headers)
resp.raise_for_status()
print(resp.json())
