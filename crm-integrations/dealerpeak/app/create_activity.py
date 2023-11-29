import logging
from os import environ
from json import loads
import requests


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

CRM_API_URL = "https://vf9p5z68u4.execute-api.us-east-1.amazonaws.com/ce-1190"
CRM_API_CREDENTIALS = {
    "partner_id": "test",
    "x_api_key": "test_6d6407b333e04fbaa42e7ca6549ff0dd"
}


class CrmApiWrapper:
    def __run_get(self, endpoint: str):
        res = requests.get(f"{CRM_API_URL}/{endpoint}", headers=CRM_API_CREDENTIALS)
        res.raise_for_status()
        return res.json()
    
    def get_lead(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}")
    
    def get_salesperson(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}/salespersons")[0]
    
    def get_consumer(self, consumer_id: int):
        return self.__run_get(f"consumers/{consumer_id}")


def lambda_handler(event: dict, context):
    crm_api = CrmApiWrapper()
    
    for event in event.get('Records', []):
        activity = loads(event['body'])
        logger.info(f"Posting {activity['activity_id']} to Dealerpeak")
        
        lead = crm_api.get_lead(activity["lead_id"])
        salesperson = crm_api.get_salesperson(activity["lead_id"])
        consumer = crm_api.get_consumer(lead["consumer_id"])
        
        crm_payload = {
            "agent": {
                "userID": salesperson['crm_salesperson_id']
            },
            "customer": {
                "userID": consumer["crm_consumer_id"]
            },
            "description": activity["notes"],
            "dueDate": activity["activity_due_ts"],
            "taskType": {
                "typeID": 1 # TODO fix
            }
        }
        
        # TODO hit dealerpeak and update our db with their activity id
        
        logger.info(crm_payload)
    
    # TODO return batch items that failed