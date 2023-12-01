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
        
    def update_activity(self, activity_id, crm_activity_id):
        res = requests.put(f"{self.url}/activities/{activity_id}", json={
            "crm_activity_id": crm_activity_id
        }, headers={
            "x_api_key": self.api_key,
            "partner_id": self.partner_id,
        })
        res.raise_for_status()
        return res.json()
    
        
class DealerpeakApiWrapper:
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = get_dealerpeak_credentials()
        self.__activity = kwargs.get("activity")
        self.__salesperson = kwargs.get("salesperson")
        self.__consumer = kwargs.get("consumer")
        self.__lead = kwargs.get("lead")
        self.__dealer_group_id = self.activity["crm_dealer_id"].split("__")[0]
        
    def __insert_note(self):
        payload = {
            "addedBy_UserID": self.__salesperson["crm_salesperson_id"],
            "leadID": self.__lead["crm_lead_id"],
            "note": self.__activity["notes"],
        }
        
        logging.info(payload)
        
        res = requests.post(f"{self.__url}/dealergroup/{self.__dealer_group_id}/customer/{self.__consumer['crm_consumer_id']}/notes", 
                            json=payload, 
                            auth=HTTPBasicAuth(self.__username, self.__password))
        
        res.raise_for_status()
        return res.json()["noteID"]
    
    def __get_task_type(self):
        return {
            "appointment": 2,
            "phone_call_task": 3,
            "outbound_call": 3,
        }[self.__activity["activity_type"]]
        
    def __insert_task(self):
        payload = {
            "agent": {
                "userID": self.__salesperson["crm_salesperson_id"]
            },
            "customer": {
                "userID": self.__consumer["crm_consumer_id"]
            },
            "description": self.__activity["notes"],
            "taskType": {
                "typeID": self.__get_task_type()
            },
            "taskResult": {}
        }
            
        if self.__activity["activity_type"] in ("appointment", "phone_call_task"):
            payload["dueDate"] = self.__activity["activity_due_ts"]
        elif self.__activity["activity_type"] == "outbound_call":
            payload["dueDate"] = self.__activity["activity_requested_ts"]
            payload["completedDate"] = self.__activity["activity_requested_ts"]
            payload["taskResult"]["resultID"] = 5
            
        logging.info(payload)
        
        res = requests.post(f"{self.url}/dealergroup/{self.__dealer_group_id}/tasks", 
                            json=payload, 
                            auth=HTTPBasicAuth(self.__username, self.__password))
        
        res.raise_for_status()
        return res.json()["task"]["taskID"]
    
    def create_activity(self):
        if self.__activity["activity_type"] == "notes":
            return self.__insert_note()
        return self.__insert_task()


def lambda_handler(event: dict, context):
    crm_api = CrmApiWrapper()
    batch_failures = []
    
    for event in event.get('Records', []):
        try:
            activity = loads(event['body'])
            lead = crm_api.get_lead(activity["lead_id"])
            salesperson = crm_api.get_salesperson(activity["lead_id"])
            consumer = crm_api.get_consumer(lead["consumer_id"])
            
            dealer_peak_api = DealerpeakApiWrapper(
                activity=activity, 
                salesperson=salesperson, 
                consumer=consumer, 
                lead=lead)
            
            dealerpeak_task_id = dealer_peak_api.create_activity()
            crm_api.update_activity(activity["activity_id"], dealerpeak_task_id)
        except:
            logger.exception(f"Failed to post activity {activity['activity_id']} to Dealerpeak")
            batch_failures.append(event['messageId'])

    return {
        "batchItemFailures": batch_failures
    }