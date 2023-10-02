"""Lead Model."""

from datetime import datetime
from ..base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, ForeignKey, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB


class Lead(BaseForModels):
    """Lead Model."""

    __tablename__ = "crm_lead"
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    crm_lead_id = Column(String)
    integration_partner_id = Column(
        Integer, ForeignKey(f'{SCHEMA}.crm_integration_partner.id')
    )
    consumer_id = Column(
        Integer, ForeignKey(f'{SCHEMA}.crm_consumer.id')
    )
    salesperson_id = Column(
        Integer, ForeignKey(f'{SCHEMA}.crm_salesperson.id')
    )
    vehicle_id = Column(
        Integer, ForeignKey(f'{SCHEMA}.crm_vehicle.id')
    )
    lead_ts = Column(DateTime)
    status = Column(String)
    substatus = Column(String)
    origin_channel = Column(String)
    source_channel = Column(String)
    request_product = Column(String)
    metadata = Column(JSONB)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    db_update_date = Column(DateTime)
    db_update_role = Column(String)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
