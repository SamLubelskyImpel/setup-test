import asyncio
import csv
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import Column, Integer, Boolean, DateTime, String
from app.orm.models.carlabs import DataImports
from app.orm.models.shared_dms import (
    VehicleSale, DealerIntegrationPartner, Dealer,
)
from app.orm.models.base_model import BaseForModels
from app.orm.connection.session import SQLSession

class PlatformDealerStore(BaseForModels):
    __tablename__ = "platform_dealer_store"
    __table_args__ = {'schema': 'public'}
    dealer_store_id = Column(Integer, primary_key=True)
    dealer_store_name = Column(String)
    dealer_group_id = Column(Integer)
    db_dealer_store_id = Column(String)
    creation_date = Column(DateTime)
    last_updated = Column(DateTime)
    active = Column(Boolean)

async def save_dealers_csv(records: list[tuple[str, str]]):
    with open('dealers.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['dealer_store_name', 'db_dealer_store_id'])
        for dealer_id, dealer_name in records:
            writer.writerow([dealer_name, dealer_id])

async def save_dealers_with_no_sales_csv(records: set[str]):
    with open('dealers_no_sales.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['impel_dealer_id'])
        for dealer in records:
            writer.writerow([dealer])

async def save_dealers_with_no_sales_and_in_carlabs_integration_data(
        dealers: set[str], carlabs_dealers: set[str]
):
    with open('carlabs_dealers.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['dealerCode'])
        for dealer in dealers:
            if dealer in carlabs_dealers:
                writer.writerow([dealer])

async def main():
    # Step 1: Retrieve current dealers from the platform_dealer_store table
    with SQLSession(db='CARLABS_ANALYTICS') as session:
        records_qs = session.query(
            PlatformDealerStore.db_dealer_store_id.distinct(),
            PlatformDealerStore.dealer_store_name,
        )
        records = records_qs.all()
        await asyncio.create_task(save_dealers_csv(records))
        db_dealer_store_ids = set([s[0] for s in records])
        print('Num dealers: ', len(db_dealer_store_ids))

    # Step 2: Identify dealers without vehicle sales records in the DMS
    with SQLSession(db='SHARED_DMS') as session:
        vehicle_sales = session.query(
            VehicleSale.dealer_integration_partner_id
        ).join(
            DealerIntegrationPartner,
            DealerIntegrationPartner.id == VehicleSale.dealer_integration_partner_id
        ).join(
            Dealer,
            Dealer.id == DealerIntegrationPartner.dealer_id
        ).where(
            Dealer.impel_dealer_id.notin_(db_dealer_store_ids)
        ).all()

        dealer_integration_partner_ids = set([
            s[0] for s in vehicle_sales if s[0] in db_dealer_store_ids
        ])

        dealers = session.query(
            Dealer.impel_dealer_id.distinct()
        ).join(
            DealerIntegrationPartner,
            DealerIntegrationPartner.dealer_id == Dealer.id
        ).where(
            DealerIntegrationPartner.id.in_(dealer_integration_partner_ids)
        ).all()

        dealers = set([s[0] for s in dealers])
        await asyncio.create_task(save_dealers_with_no_sales_csv(dealers))
        print('Num. of dealers without sales: ', len(dealers))

    # Step 3: Check JSON dump source for missing dealers in the data_integrations table
    with SQLSession(db='CARLABS_DATA_INTEGRATIONS') as session:
        carlabs_data = session.query(
            DataImports.dealerCode,
        ).where(
            DataImports.dealerCode.in_(dealers)
        ).all()
        carlabs_dealers = set([s[0] for s in carlabs_data])

    # Step 4: Save dealers with no sales and existing in Carlabs data
    await asyncio.create_task(
        save_dealers_with_no_sales_and_in_carlabs_integration_data(
            dealers,
            carlabs_dealers
        )
    )
    print(
        'Num. of dealers with no sales and existing in Carlabs: ',
        len(carlabs_dealers)
    )

if __name__ == '__main__':
    asyncio.run(main())
