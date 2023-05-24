from abc import ABC, abstractmethod
from orm.models.shared_dms import VehicleSale, Consumer, Vehicle, ServiceContract
from pandas import DataFrame, json_normalize
from datetime import datetime, date
from mappings import VehicleSaleTableMapping, ConsumerTableMapping, VehicleTableMapping, ServiceContractTableMapping, BaseMapping
from numpy import int64

class BaseTransformer(ABC):

    vehicle_sale_table_mapping: VehicleSaleTableMapping
    consumer_table_mapping: ConsumerTableMapping
    vehicle_table_mapping: VehicleTableMapping
    service_contract_table_mapping: ServiceContractTableMapping
    carlabs_data: DataFrame
    date_format: str

    def __init__(self, carlabs_data: dict) -> None:
        assert self.vehicle_sale_table_mapping is not None
        assert self.consumer_table_mapping is not None
        assert self.vehicle_table_mapping is not None
        assert self.service_contract_table_mapping is not None

        self.carlabs_data = json_normalize(carlabs_data).iloc[0]
        self.pre_process_data()

    @abstractmethod
    def pre_process_data(self):
        ...

    @abstractmethod
    def post_process_consumer(self, orm: Consumer) -> Consumer:
        ...

    @abstractmethod
    def post_process_vehicle_sale(self, orm: VehicleSale) -> VehicleSale:
        ...

    def parsed_date(self, raw_date: str) -> date:
        try:
            return datetime.strptime(raw_date, self.date_format)
        except Exception:
            return None

    def __parsed(self, v):
        if isinstance(v, int64):
            return int(v)
        return v

    def __get_mapped_data(self, mapping: BaseMapping):
        return { dms_field: self.__parsed(self.carlabs_data.get(carlabs_field)) for dms_field, carlabs_field in mapping.fields }

    @property
    def vehicle_sale(self) -> VehicleSale:
        mapped = self.__get_mapped_data(self.vehicle_sale_table_mapping)

        orm = VehicleSale(**mapped)

        orm.sale_date = self.parsed_date(orm.sale_date)
        orm.date_of_inventory = self.parsed_date(orm.date_of_inventory)
        orm.date_of_state_inspection = self.parsed_date(orm.date_of_state_inspection)
        orm.warranty_expiration_date = self.parsed_date(orm.warranty_expiration_date)
        orm.delivery_date = self.parsed_date(orm.delivery_date)

        if orm.sale_date and orm.date_of_inventory:
            orm.days_in_stock = (orm.sale_date - orm.date_of_inventory).days

        orm.has_service_contract = orm.has_service_contract is not None and len(orm.has_service_contract) > 0
        orm.service_package_flag = orm.service_package_flag is not None and len(orm.service_package_flag) > 0

        return self.post_process_vehicle_sale(orm)

    @property
    def consumer(self) -> Consumer:
        mapped = self.__get_mapped_data(self.consumer_table_mapping)
        orm = Consumer(**mapped)
        return self.post_process_consumer(orm)

    @property
    def vehicle(self) -> Vehicle:
        mapped = self.__get_mapped_data(self.vehicle_table_mapping)
        orm = Vehicle(**mapped)
        return orm

    @property
    def service_contract(self) -> ServiceContract:
        mapped = self.__get_mapped_data(self.service_contract_table_mapping)
        orm = ServiceContract(**mapped)
        orm.start_date = self.parsed_date(orm.start_date)
        return orm

