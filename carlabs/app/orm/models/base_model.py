from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import TypeDecorator, Float
from sqlalchemy.dialects import postgresql


class BaseForModels(DeclarativeBase):
    ...


class DoublePrecisionField(TypeDecorator):
    impl = postgresql.DOUBLE_PRECISION  # Specify the underlying database-specific floating-point type

    def process_bind_param(self, value, dialect):
        if value == '':
            return 0
        if isinstance(value, str):
            return float(value.replace(',', ''))
        return value