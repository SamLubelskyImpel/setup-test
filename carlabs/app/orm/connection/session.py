from builtins import object
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import traceback
from .make_db_uri import make_db_uri
from typing import Literal


_Sessions = {}

# engine = create_engine(make_db_uri())


class SQLSession(object):
    def __init__(self, db: Literal['CARLABS_DATA_INTEGRATIONS', 'SHARED_DMS'], region=None, pool_size=5):
        self.uri = make_db_uri(db, region)
        if self.uri in _Sessions:
            self.engine = _Sessions[self.uri][0]
            self.Session = _Sessions[self.uri][1]
        else:
            self.engine = create_engine(
                self.uri,
                pool_size=pool_size,
                pool_recycle=300
            )
            self.Session = sessionmaker(bind=self.engine)
            _Sessions[self.uri] = (self.engine, self.Session)
        self.__state = 'pre-open'

    def __enter__(self):
        if self.__state != 'pre-open':
            raise RuntimeError(
                'this session was already used in a context manager')
        self.session = self.Session()
        self.__state = 'open'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__state = 'closed'
        try:
            swallow = False
            try:
                if exc_type:
                    # A real exception caused this. Don't raise a new one.
                    swallow = True
                    self.session.rollback()
                else:
                    self.session.commit()
            except Exception:
                if swallow:
                    # There is nothing else we can do here.
                    traceback.print_exc()
                else:
                    raise
        finally:
            try:
                self.session.close()
            except Exception:
                traceback.print_exc()  # There is nothing else we can do here.

    def __getattr__(self, name):
        if self.__state != 'open':
            raise RuntimeError('using session that is not open')
        return getattr(self.session, name)

    def query(self, *args, **kwds):
        # Return a new query object that uses this wrapper as the session.
        return self.session.query(*args, **kwds).with_session(self)
