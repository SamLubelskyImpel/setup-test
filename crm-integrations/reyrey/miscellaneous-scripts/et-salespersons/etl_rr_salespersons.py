import json


with open("input.json", "r") as f:
    salespersons = json.load(f)["RecordList"]

crm_users = []
error_list = []

for item in salespersons:
    try:
        if "," in item["SelectedLoginUserFullName"]:
            fn_ln = item["SelectedLoginUserFullName"].split(",")
            fn = fn_ln[1].replace(" ", "")
            ln = fn_ln[0].replace(" ", "")
        else:
            fn_ln = item["SelectedLoginUserFullName"].replace(" ", "")
            fn = fn_ln
            ln = ""

        sp = {}
        sp["Emails"] = []
        sp["Phones"] = []
        sp["FullName"] = item["SelectedLoginUserFullName"]
        sp["FirstName"] = fn
        sp["LastName"] = ln
        sp["UserId"] = fn + ln

        crm_users.append(sp)
    except Exception as e:
        print(e)
        print("Error while ETL")
        error_list.append(item)

with open("output.json", "w") as f:
    json.dump(crm_users, f, indent=4)

with open("error.json", "w") as f:
    json.dump(error_list, f, indent=4)