# AWS_PROFILE=unified-test AWS_DEFAULT_REGION=us-east-1 ENVIRONMENT=carlabs-etl python app/import_dip.py

from orm.connection.session import SQLSession
from orm.models.carlabs import DataImports
from orm.models.shared_dms import DealerIntegrationPartner
from orm.models.shared_dms import IntegrationPartner
from datetime import datetime

from orm.models.base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey


class Dealer(BaseForModels):
    """Dealer Model."""

    __tablename__ = "dealer"
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    impel_dealer_id = Column(String)
    db_creation_date = Column(DateTime)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }

id = 205

with SQLSession(db='CARLABS_DATA_INTEGRATIONS') as session:
    records = session.query(DataImports.dealerCode.distinct(), DataImports.dataSource).where(DataImports.dataType == 'SALES')
    with SQLSession(db='SHARED_DMS') as dms_session:
        for record in records:
            ip = dms_session.query(
                IntegrationPartner
            ).where(
                IntegrationPartner.impel_integration_partner_id.ilike(record[1])
            ).first()

            if not ip:
                print(f'IP not found for {record}')
                continue

            dealer = Dealer(
                impel_dealer_id=f'test_impel_dealer_{id}',
                db_creation_date=datetime.utcnow()
            )

            dip = DealerIntegrationPartner(
                integration_partner_id=ip.id,
                dealer=dealer,
                dms_id=record[0],
                is_active=True,
                db_creation_date=datetime.utcnow()
            )

            dms_session.add(dip)

            id += 1