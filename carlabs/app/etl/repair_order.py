from dataclasses import dataclass, field
from datetime import date
from orm.connection.session import SQLSession
from orm.models.carlabs import RepairOrder
from orm.models.shared_dms import ServiceRepairOrder, Consumer, Vehicle
from mapping.repair_order import map_service_repair_order, map_consumer, map_vehicle
from utils import save_progress, publish_failure, get_dealer_integration_partner_id
import traceback


@dataclass
class TransformedData:
    consumer: Consumer
    vehicle: Vehicle
    repair_order: ServiceRepairOrder


@dataclass
class RepairOrderETL:

    last_id: str
    day: date
    limit: int

    _finished: bool = field(init=False, default=False)
    _loaded: int = field(init=False, default=0)
    _failed: int = field(init=False, default=0)

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
        with SQLSession(db='CARLABS_ANALYTICS') as carlabs_session:
            records = carlabs_session.query(RepairOrder).where(
                (RepairOrder.db_creation_date > self.day) &
                (RepairOrder.id > self.last_id)
            ).order_by(
                RepairOrder.id.asc()
            ).limit(self.limit + 1)

            self._finished = records.count() <= self.limit

            for r in records[:self.limit]:
                yield r

    def _load_into_dms(self, transformed: TransformedData):
        with SQLSession(db='SHARED_DMS') as dms_session:
            transformed.repair_order.consumer = transformed.consumer
            transformed.repair_order.vehicle = transformed.vehicle

            dms_session.add(transformed.consumer)
            dms_session.add(transformed.vehicle)
            dms_session.add(transformed.repair_order)

    def _transform(self, record: RepairOrder):
        dip_id = get_dealer_integration_partner_id(
            dealer_code=record.dealer_id,
            data_source=record.ro_source)
        return TransformedData(
            consumer=map_consumer(record, dip_id),
            vehicle=map_vehicle(record, dip_id),
            repair_order=map_service_repair_order(record, dip_id)
        )

    def run(self):
        records = self._extract_from_carlabs()
        for r in records:
            try:
                transformed = self._transform(r)
                self._load_into_dms(transformed)
                self._loaded += 1
            except Exception:
                publish_failure(
                    record=r.as_dict(),
                    err=traceback.format_exc(),
                    table='repair_order')
                self._failed += 1
            finally:
                save_progress(r.id, 'repair_order_progress')
