from rds_instance import RDSInstance
import json

def load_data(file_name):
    print("Loading data from file:", file_name)  # Debugging statement
    with open(file_name, 'r') as file:
        # Step 3: Load the JSON data
        json_data_list = json.load(file)
    print("Data loaded successfully")  # Debugging statement
    return json_data_list


def extract_option_data(option_json):
    option_data = {
        'option_description': option_json.get('inv_option|option_description', ''),
        'is_priority': option_json.get('inv_option|is_priority', False)
    }
    return option_data


if __name__ == "__main__":
    print("Initializing RDSInstance")  # Debugging statement
    rds_instance = RDSInstance()
    print("RDSInstance initialized")  # Debugging statement

    json_data_list = load_data('5058.json')

    json_data = json_data_list[0]

    # print(json_data.get('inv_options|inv_options'))

    # Insert options data
    if json_data.get('inv_options|inv_options'):
        option_data_list = []
        for option_json in json_data['inv_options|inv_options']:
            option_data = extract_option_data(option_json)
            option_data_list.append(option_data)

    # print("Options Data:")
    # print(option_data_list)

    existing_records = rds_instance.bulk_check_existing_records(
        table="inv_option",
        check_columns=["option_description", "is_priority"],
        data_list=option_data_list,
    )

    print("Existing Records:")
    print(existing_records)
