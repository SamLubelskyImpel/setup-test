from sqlalchemy import Column, Integer, String
from ..base_model import BaseForModels, SCHEMA


class Dealer(BaseForModels):
    '''Dealer Model.'''

    __tablename__ = 'dealer'
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    impel_dealer_id = Column(String)


    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
