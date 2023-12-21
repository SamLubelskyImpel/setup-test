import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from sqlalchemy import func, text

from dms_orm.models.appointment import Appointment
from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.service_contract import ServiceContract
from dms_orm.models.op_code import OpCode
from dms_orm.models.op_code_appointment import OpCodeAppointment
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def convert_dt(value):
    """ Convert date or datetime string into datetime object """
    if "T" in value:
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
    else:
        return datetime.strptime(value, '%Y-%m-%d')


def filter_query(query, filters, tables):
    """Function filters the query based on filters."""
    for attr, value in filters.items():
        if attr == "appointment_date_start":
            query = query.filter(getattr(Appointment, "appointment_date") >= convert_dt(value))
        elif attr == "appointment_date_end":
            query = query.filter(getattr(Appointment, "appointment_date") <= convert_dt(value))
        elif attr == "db_creation_date_start":
            query = query.filter(getattr(Appointment, "db_creation_date") >= convert_dt(value))
        elif attr == "db_creation_date_end":
            query = query.filter(getattr(Appointment, "db_creation_date") <= convert_dt(value))
        elif attr == "db_update_date_start":
            query = query.filter(getattr(Appointment, "db_update_date") >= convert_dt(value))
        elif attr == "db_update_date_end":
            query = query.filter(getattr(Appointment, "db_update_date") <= convert_dt(value))
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
        max_results = 100
        result_count = (
            max_results
            if not filters
            else int(filters.get("result_count", max_results))
        )
        max_results = min(max_results, result_count)

        with DBSession() as session:
            query = (
                session.query(
                    Appointment,
                    Consumer,
                    DealerIntegrationPartner,
                    Dealer,
                    IntegrationPartner,
                    Vehicle,
                    func.jsonb_agg(func.DISTINCT(text("service_contracts.*")).label(
                        "service_contracts"
                    )),
                    func.jsonb_agg(func.DISTINCT(text("op_code.*")).label("op_codes")),
                )
                .outerjoin(
                    DealerIntegrationPartner,
                    Appointment.dealer_integration_partner_id
                    == DealerIntegrationPartner.id,
                )
                .outerjoin(Consumer, Appointment.consumer_id == Consumer.id)
                .outerjoin(Vehicle, Appointment.vehicle_id == Vehicle.id)
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(
                    IntegrationPartner,
                    DealerIntegrationPartner.integration_partner_id
                    == IntegrationPartner.id,
                )
                .outerjoin(
                    ServiceContract,
                    ServiceContract.appointment_id == Appointment.id,
                )
                .outerjoin(
                    OpCodeAppointment,
                    OpCodeAppointment.appointment_id == Appointment.id,
                )
                .outerjoin(
                    OpCode,
                    OpCode.id == OpCodeAppointment.op_code_id,
                )
                .group_by(
                    Appointment.id,
                    DealerIntegrationPartner.id,
                    Consumer.id,
                    Vehicle.id,
                    Dealer.id,
                    IntegrationPartner.id,
                )
            )

            if filters:
                query = filter_query(
                    query,
                    filters,
                    [
                        Appointment,
                        DealerIntegrationPartner,
                        Consumer,
                        Vehicle,
                        Dealer,
                        IntegrationPartner,
                    ],
                )

            appointments = (
                query.order_by(Appointment.id)
                .limit(max_results + 1)
                .offset((page - 1) * max_results)
                .all()
            )

            results = []

            for (
                appointment,
                consumer,
                dealer_integration_partner,
                dealer,
                integration_partner,
                vehicle,
                service_contracts,
                op_codes,
            ) in appointments[:max_results]:
                result_dict = appointment.as_dict()
                result_dict["consumer"] = consumer.as_dict()
                result_dict[
                    "dealer_integration_partner"
                ] = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                result_dict["vehicle"] = vehicle.as_dict()
                result_dict["service_contracts"] = [x for x in service_contracts if x]
                result_dict["op_codes"] = [x for x in op_codes if x]
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
