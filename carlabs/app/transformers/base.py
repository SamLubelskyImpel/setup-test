from abc import ABC, abstractmethod
from ..orm.models.shared_dms.vehicle_sale import VehicleSale
from ..orm.models.shared_dms.consumer import Consumer
from pandas import DataFrame, json_normalize
from datetime import datetime, date
from ..mappings.vehicle_sale import VehicleSaleTableMapping
from ..mappings.consumer import ConsumerTableMapping
from ..mappings.base import BaseMapping


class BaseTransformer(ABC):

    vehicle_sale_table_mapping: VehicleSaleTableMapping
    consumer_table_mapping: ConsumerTableMapping
    carlabs_data: DataFrame
    date_format: str

    def __init__(self, carlabs_data: DataFrame) -> None:
        assert self.vehicle_sale_table_mapping is not None
        assert self.consumer_table_mapping is not None

        self.carlabs_data = json_normalize(carlabs_data.to_dict()).iloc[0]
        self.pre_process_data()

    @abstractmethod
    def pre_process_data(self):
        ...

    def parsed_date(self, raw_date: str) -> date:
        try:
            return datetime.strptime(raw_date, self.date_format)
        except Exception:
            return None

    def __get_mapped_data(self, mapping: BaseMapping):
        mapped = {}

        for dms_field, carlabs_field in mapping.fields:
            mapped[dms_field] = self.carlabs_data[carlabs_field]

        return mapped

    @property
    def vehicle_sale(self):
        mapped = self.__get_mapped_data(self.vehicle_sale_table_mapping)

        orm = VehicleSale(**mapped)

        sale_date = self.parsed_date(orm.sale_date)
        date_of_inventory = self.parsed_date(orm.date_of_inventory)

        if sale_date and date_of_inventory:
            orm.days_in_stock = (sale_date - date_of_inventory).days

        orm.has_service_contract = orm.has_service_contract is not None and len(orm.has_service_contract) > 0
        orm.service_package_flag = orm.service_package_flag is not None and len(orm.service_package_flag) > 0
        orm.si_load_timestamp = datetime.utcnow()
        # TODO what to put here
        orm.si_load_process = ''

        return orm

    # TODO add logic to deal with optin flags
    @property
    def consumer(self):
        mapped = self.__get_mapped_data(self.consumer_table_mapping)

        orm = Consumer(**mapped)

        orm.si_load_timestamp = datetime.utcnow()
        # TODO what to put here
        orm.si_load_process = ''

        return orm
