from json import loads
from logging import getLogger

from aws_lambda_powertools.utilities.batch import (
    BatchProcessor, process_partial_response, EventType
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext

from .envs import LOG_LEVEL
from .api_wrappers import TekionApiWrapper, CrmApiWrapper, InvalidLeadException, InvalidNoteException
from .schemas import SendActivityEvent

logger = getLogger()
logger.setLevel(LOG_LEVEL.upper())

crm_api = CrmApiWrapper()

def record_handler(record: SQSRecord):
    """Create activity on Tekion."""
    logger.info(
        f"Processing record: {record.message_id}", extra={"record": record}
    )
    body = loads(record.body)
    activity_event = SendActivityEvent(**body)
    try:
        tekion_wrapper = TekionApiWrapper(activity_event)
        tekion_activity_id = tekion_wrapper.create_activity()
        logger.info(f"Tekion response with Activity ID: {tekion_activity_id}")
        crm_api.update_activity(activity_event.activity_id, tekion_activity_id)
    except InvalidLeadException as e:
        logger.error(f"Invalid lead data: {e.message}")
    except InvalidNoteException as e:
        logger.error(e.message)
    except Exception as e:
        logger.exception(f"Failed to post activity {activity_event.activity_id} to Tekion")
        logger.error("[SUPPORT ALERT] Failed to Send Activity [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nActivityId: {}\nActivityType: {}\nTraceback: {}".format(
            activity_event.dealer_integration_partner_id, activity_event.lead_id, activity_event.activity_id, activity_event.activity_type, e)
            )
        raise


def lambda_handler(event, context: LambdaContext):
    """Create activity on Tekion."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )

        return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise

__all__ = ['lambda_handler', 'record_handler']