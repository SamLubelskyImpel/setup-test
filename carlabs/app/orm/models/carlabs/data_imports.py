from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import Column, Integer, JSON, String, DateTime
from ..base_model import BaseForModels


class DataImports(BaseForModels):
    __tablename__ = 'dataImports'

    id = Column(Integer, primary_key=True)
    dealerCode = Column(String)
    importedData = Column(JSON)
    dataSource = Column(String)
    dataType = Column(String)
    creationDate = Column(DateTime)

    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
