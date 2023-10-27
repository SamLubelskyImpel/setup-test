import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.vehicle import Vehicle
from dms_orm.models.consumer import Consumer
from dms_orm.models.appointment import Appointment
from dms_orm.models.dealer import Dealer
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.service_contract import ServiceContract
from dms_orm.session_config import DBSession
from sqlalchemy import func, text
from sqlalchemy.orm import aliased


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def filterQuery(query, filters, tables):
    if filters:
        for attr, value in filters.items():
            if attr == "appointment_date":
                query = query.filter(
                    getattr(Appointment, "appointment_date") == value
                )
            elif attr == "db_creation_date_start":
                query = query.filter(
                    getattr(Appointment, "db_creation_date") >= value
                )
            elif attr == "db_creation_date_end":
                query = query.filter(
                    getattr(Appointment, "db_creation_date") <= value
                )
            else:
                filtered_table = None
                for table in tables:
                    if attr in table.__table__.columns:
                        filtered_table = table

                if not filtered_table:
                    continue
                query = query.filter(getattr(filtered_table, attr) == value)
    return query

def lambda_handler(event, context):
    """Run appointment API."""
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

            service_contracts_1 = aliased(ServiceContract)

            subquery = (
                session.query(
                    Appointment.id.label("id"),
                    func.jsonb_agg(text("service_contracts_1")).label("service_contracts_list"),
                )
                .outerjoin(DealerIntegrationPartner, Appointment.dealer_integration_partner_id == DealerIntegrationPartner.id)
                .outerjoin(Consumer, Appointment.consumer_id == Consumer.id)
                .outerjoin(Vehicle, Appointment.vehicle_id == Vehicle.id)
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id)
                .join(
                    service_contracts_1,
                    service_contracts_1.appointment_id == Appointment.id,
                )
                .group_by(Appointment.id)
            )

            if filters:
                subquery = filterQuery(subquery, filters, [
                    Appointment, 
                    DealerIntegrationPartner,
                    Consumer,
                    Vehicle,
                    Dealer,
                    IntegrationPartner,
                    service_contracts_1
                ])     
            
            subquery = subquery.subquery()

            query = (
                session.query(Appointment, Consumer, Vehicle, subquery.c.service_contracts_list)
                .outerjoin(DealerIntegrationPartner, Appointment.dealer_integration_partner_id == DealerIntegrationPartner.id)
                .outerjoin(Consumer, Appointment.consumer_id == Consumer.id)
                .outerjoin(Vehicle, Appointment.vehicle_id == Vehicle.id)
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id)
                .outerjoin(subquery, subquery.c.id == Appointment.id)
            )

            if filters: 
                query = filterQuery(query, filters, [
                    Appointment,
                    DealerIntegrationPartner,
                    Consumer,
                    Vehicle,
                    Dealer,
                    IntegrationPartner
                ])

            print(query)

            
            appointments = (
                    query.order_by(Appointment.db_creation_date)
                    .limit(max_results + 1)
                    .offset((page - 1) * max_results)
                    .all()
                )
            
            results = []

            for(appointment, consumer, vehicle, service_contracts_list) in appointments[:max_results]:
                result_dict = appointment.as_dict()
                result_dict['consumer'] = consumer.as_dict()
                result_dict['vehicle'] = vehicle.as_dict()
                result_dict['service_contracts_list'] = service_contracts_list
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

print(lambda_handler({"queryStringParameters":{
    "vin": "5XYPG4A5XG1504185"
}}, None))