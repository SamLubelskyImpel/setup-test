import logging
from os import environ
from json import loads
import requests
from requests.auth import HTTPBasicAuth
from utils import get_dealerpeak_credentials, get_crm_api_credentials


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class CrmApiWrapper:
    def __init__(self) -> None:
        self.url, self.partner_id, self.api_key = get_crm_api_credentials()
        
    def __run_get(self, endpoint: str):
        res = requests.get(f"{self.url}/{endpoint}", headers={
            "x_api_key": self.api_key,
            "partner_id": self.partner_id,
        })
        res.raise_for_status()
        return res.json()
    
    def get_lead(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}")
    
    def get_salesperson(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}/salespersons")[0]
    
    def get_consumer(self, consumer_id: int):
        return self.__run_get(f"consumers/{consumer_id}")
        
        
class DealerpeakApiWrapper:
    def __init__(self):
        self.url, self.username, self.password = get_dealerpeak_credentials()

    def create_activity(self, payload: dict, dealer_id: int):
        dealer_group_id = dealer_id.split("__")[0]
        res = requests.post(f"{self.url}/dealergroup/{dealer_group_id}/tasks", 
                            json=payload, 
                            auth=HTTPBasicAuth(self.username, self.password))
        res.raise_for_status()
        return res.json()["task"]["taskID"]


def lambda_handler(event: dict, context):
    crm_api = CrmApiWrapper()
    dealer_peak_api = DealerpeakApiWrapper()
    batch_failures = []
    
    for event in event.get('Records', []):
        try:
            activity = loads(event['body'])
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
            
            dealerpeak_task_id = dealer_peak_api.create_activity(crm_payload, dealer_id=activity["crm_dealer_id"])
            
            logger.info(dealerpeak_task_id)
            
            # TODO update activity with dealerpeak_task_id
        except:
            logger.exception(f"Failed to post activity {activity['activity_id']} to Dealerpeak")
            batch_failures.append(event['messageId'])
        
    
    return {
        "batchItemFailures": batch_failures
    }