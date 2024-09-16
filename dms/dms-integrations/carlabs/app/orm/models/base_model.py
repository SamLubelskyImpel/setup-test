from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import TypeDecorator
from sqlalchemy.dialects import postgresql
import os


SCHEMA = 'prod' if os.environ.get('ENVIRONMENT') == 'prod' else 'stage'


class BaseForModels(DeclarativeBase):
    ...


class DoublePrecisionField(TypeDecorator):
    impl = postgresql.DOUBLE_PRECISION

    def process_bind_param(self, value, dialect):
        if value == '':
            return 0
        if isinstance(value, str):
            return float(value.replace(',', ''))
        return value
