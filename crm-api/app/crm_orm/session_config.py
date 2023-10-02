"""Create reusable sqlalchemy session object."""

import sys
import traceback
from builtins import object
from os import environ

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

assert environ["ENVIRONMENT"], "Environment variable `ENVIRONMENT` was not defined"

env = environ["ENVIRONMENT"]


from crm_orm.create_db_uri import create_db_uri

_Sessions = {}
BaseForModels = declarative_base(
    metadata=MetaData(schema="prod" if env == "prod" else "stage")
)


class DBSession(object):
    """Create reusable sqlalchemy session object."""

    def __init__(self):
        """Create DB Session."""
        self.uri = create_db_uri(env)
        if self.uri in _Sessions:
            self.engine = _Sessions[self.uri][0]
            self.Session = _Sessions[self.uri][1]
        else:
            self.engine = create_engine(
                self.uri,
                # TODO: unsure how long connections are open for in our database (look into this)
                pool_recycle=300,
                connect_args={"options": "-c timezone=UTC"}
            )
            self.Session = sessionmaker(bind=self.engine)
            _Sessions[self.uri] = (self.engine, self.Session)
        self.__state = "pre-open"

    def __enter__(self):
        """Initialize DB Session."""
        if self.__state != "pre-open":
            raise RuntimeError("this session was already used in a context manager")
        self.session = self.Session()
        self.__state = "open"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close DB Session."""
        self.__state = "closed"
        try:
            self.session.close()
        except Exception:
            traceback.print_exc()

    def __getattr__(self, name):
        """Return DB session attributes."""
        if self.__state != "open":
            raise RuntimeError("using session that is not open")
        return getattr(self.session, name)

    def query(self, *args, **kwds):
        """Return a new query object that uses this wrapper as the session."""
        return self.session.query(*args, **kwds).with_session(self)
