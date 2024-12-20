from repair_orders import parse_data
from datetime import datetime, timedelta


def main():
    start_date_str = "2024-12-04T00:00:00"
    end_date_str = "2024-12-18T00:00:00"
    start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S")

    sqs_message_data_mock = {
        "dealer_id": "1119" #treptau repair llc - dms_id 611-843, dealer_integration_partner.id 1172
    }
    
    while start_date <= end_date:
        sqs_message_data_mock["end_dt_str"] = start_date.strftime("%Y-%m-%d")

        print(start_date.strftime("%Y-%m-%d"))
        parse_data(sqs_message_data_mock)

        start_date += timedelta(days=1)
    


if __name__ == "__main__":
    main()
