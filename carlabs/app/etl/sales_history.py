from orm.connection.session import SQLSession
from orm.models.carlabs import DataImports
from datetime import date
from mapping.sales_history import map_service_contract, map_consumer, map_sale, map_vehicle
from orm.models.shared_dms import Consumer, Vehicle, VehicleSale, ServiceContract
from dataclasses import dataclass, field
from utils import save_progress, publish_failure, get_dealer_integration_partner_id
import traceback
from sqlalchemy.sql import func
from sqlalchemy import Integer
from datetime import datetime


@dataclass
class TransformedData:
    consumer: Consumer
    vehicle: Vehicle
    sale: VehicleSale
    service_contract: ServiceContract


@dataclass
class SalesHistoryETL:

    last_id: str
    limit: int

    _finished: bool = field(init=False, default=False)
    _loaded: int = field(init=False, default=0)
    _failed: int = field(init=False, default=0)
    _started_at: datetime = field(init=False, default_factory=datetime.utcnow)
    _extracted: int = field(init=False, default=0)

    @property
    def finished(self):
        return self._finished

    @property
    def loaded(self):
        return self._loaded

    @property
    def failed(self):
        return self._failed

    def _extract_from_carlabs(self):
        while self._extracted < self.limit:
            with SQLSession(db='CARLABS_DATA_INTEGRATIONS') as carlabs_session:
                records = carlabs_session.query(DataImports).where(
                    (DataImports.dataType == 'SALES') &
                    (DataImports.id > self.last_id)
                ).order_by(
                    DataImports.id.asc()
                ).offset(self._extracted).limit(1)

                record = records[0]

                if not record:
                    self.finished = True
                    return

                if isinstance(record.importedData, list):
                    for i_data in record.importedData:
                        di_copy = DataImports(**record.as_dict())
                        di_copy.importedData = i_data
                        yield di_copy
                else:
                    yield record

            self._extracted += 1

    def _load_into_dms(self, transformed: TransformedData):
        with SQLSession(db='SHARED_DMS') as dms_session:
            transformed.sale.vehicle = transformed.vehicle
            transformed.sale.consumer = transformed.consumer

            transformed.service_contract.consumer = transformed.consumer
            transformed.service_contract.vehicle = transformed.vehicle

            dms_session.add(transformed.consumer)
            dms_session.add(transformed.vehicle)
            dms_session.add(transformed.sale)

            if transformed.sale.has_service_contract:
                dms_session.add(transformed.service_contract)

    def _transform(self, record: DataImports):
        dip_id = get_dealer_integration_partner_id(
            dealer_code=record.dealerCode,
            data_source=record.dataSource)

        return TransformedData(
            consumer=map_consumer(record, dip_id),
            vehicle=map_vehicle(record, dip_id),
            sale=map_sale(record, dip_id),
            service_contract=map_service_contract(record, dip_id)
        )

    def run(self):
        records = self._extract_from_carlabs()
        for r in records:
            # Break execution if it reaches 10 mins
            if (datetime.utcnow() - self._started_at).seconds >= 600:
                return

            try:
                transformed = self._transform(r)
                self._load_into_dms(transformed)
                self._loaded += 1
            except Exception:
                publish_failure(
                    record=r.as_dict(),
                    err=traceback.format_exc(),
                    table='dataImports')
                self._failed += 1
            finally:
                save_progress(r.id, 'sales_history_progress')
