from builtins import object
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import traceback

from .create_db_uri import create_db_uri

BaseForModels = declarative_base()


class DBSession(object):


    def __init__(self):   
        # create engine. ref create db uri. check session to see if one is open.
        pass
    
    def __enter__(self):
        # define self.session variable to sessionmaker class to insantiate.
        self.session = self.Session()
        self.__state = 'open'
        pass
    
    def __exit__(self):
        # close and commit session.
        pass
    
    def query(self):
        """Return a new query object that uses this wrapper as the session."""
        pass