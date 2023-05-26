from dataclasses import dataclass, field
from datetime import date
from orm.connection.session import SQLSession
from orm.models.repair_order import RepairOrder
from orm.models.service_repair_order import ServiceRepairOrder
from mapping import map_service_repair_order
from utils import save_progress, publish_failure
import traceback


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
            records: list[RepairOrder] = carlabs_session.query(RepairOrder).where(
                (RepairOrder.db_creation_date > self.day) &
                (RepairOrder.id > self.last_id)
            ).order_by(
                RepairOrder.id.asc()
            ).limit(self.limit+1)

            self._finished = records.count() <= self.limit

            for r in records[:self.limit]:
                yield r


    def _load_into_dms(self, transformed: ServiceRepairOrder):
        with SQLSession(db='SHARED_DMS') as dms_session:
            dms_session.add(transformed)


    def _transform(self, record: RepairOrder):
        return map_service_repair_order(record)


    def run(self):
        records = self._extract_from_carlabs()
        for r in records:
            try:
                transformed = self._transform(r)
                self._load_into_dms(transformed)
                self._loaded += 1
            except Exception:
                publish_failure(r.as_dict(), traceback.format_exc(), 'repair_order')
                self._failed +=1
            finally:
                save_progress(r.id, 'repair_order_progress')