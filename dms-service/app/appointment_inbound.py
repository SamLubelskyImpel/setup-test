import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.vehicle import Vehicle
from dms_orm.models.consumer import Consumer
from dms_orm.models.appointment import Appointment
from dms_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def lambda_handler(event, context):
    """Run dealer API."""
    logger.info(f"Event: {event}")
    
    try:
        filters = event.get("queryStringParameters", {})
        page = 1 if not filters else int(filters.get("page", "1"))
        results = []
        max_results = 1000
        result_count = (
            max_results
            if not filters
            else int(filters.get("result_count", max_results))
        )
        max_results = min(max_results, result_count)

        with DBSession() as session:
            query = (
                session.query(Appointment, DealerIntegrationPartner, Consumer, Vehicle)
                .outerjoin(DealerIntegrationPartner, Appointment.dealer_integration_partner_id == DealerIntegrationPartner.id)
                .outerjoin(Consumer, Appointment.consumer_id == Consumer.id)
                .outerjoin(Vehicle, Appointment.vehicle_id == Vehicle.id)
            )

            if filters:
                    tables = [Appointment, DealerIntegrationPartner, Consumer, Vehicle]
            

            appointments = (
                    query.order_by(Appointment.db_creation_date)
                    .limit(max_results + 1)
                    .offset((page - 1) * max_results)
                    .all()
                )
            
            results = []

            for(appointment, dealer_integration_partner, consumer, vehicle) in appointments[:max_results]:
                result_dict = appointment.as_dict()
                result_dict['dealer_integration_partner'] = dealer_integration_partner.as_dict()
                result_dict['consumer'] = consumer.as_dict()
                result_dict['vehicle'] = vehicle.as_dict()
                results.append(result_dict)
             
        
        return {
                "statusCode": "200",
                "body": dumps(
                    {
                        "received_date_utc": datetime.utcnow()
                        .replace(microsecond=0)
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                        "results": results,
                        "has_next_page": len(appointments) > max_results,
                    },
                    default=json_serial,
                ),
            }
    except Exception:
        logger.exception("Error running appointment api.")
        raise

print(lambda_handler({}, None))