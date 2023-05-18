from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import Column, Integer, JSON, String, DateTime
from .base_model import BaseForModels


class DataImports(BaseForModels):
    __tablename__ = 'dataImports'

    id = Column(Integer, primary_key=True)
    dealer_code = Column('dealerCode', String)
    imported_data = Column('importedData', JSON)
    data_source = Column('dataSource', String)
    data_type = Column('dataType', String)
    creation_date = Column('creationDate', DateTime)