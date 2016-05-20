## defines a scoped session for creating thread local session by calling self.DBSession
# 
# See: http://docs.sqlalchemy.org/en/latest/orm/session_basics.html#what-does-the-session-do for
# more info about sessions?
# 
# See: http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html#contextual-thread-local-sessions for
# thread local sessions which have to be using in web environments or other multithread
# applications.

import os
import ConfigParser
import sqlalchemy.orm

###### libraries are in the same folder
#####from DBConfig import DBConfig
#####
###### use default config file
#####etcpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../etc' ) )
#####
###### default config file for database connection
#####configFileName = "{etcPath}/db.cfg".format(etcPath=etcpath)
#####
###### read config file
#####dbConfig = DBConfig( configFileName=configFileName )
#####config = dbConfig.config
#####
#####databaseDialect = config.get( 'DATABASE', 'database_dialect' )
#####databaseHost = config.get( 'DATABASE', 'database_host' )
#####databasePort = config.get( 'DATABASE', 'database_port' )
#####databaseName = config.get( 'DATABASE', 'database_name' )
#####databaseUsername = config.get( 'DATABASE', 'database_username' )
#####databasePassword = config.get( 'DATABASE', 'database_password' )
#####try:
#####    echo = config.getboolean( 'DATABASE', 'echo' )
#####except:
#####    echo = False


# library is in the same folder
from hDatabase import Base

# use default config file
etcpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../etc' ) )

# default config file for database connection
configFileName = "{etcPath}/serversettings.cfg".format(etcPath=etcpath)

# read config file
if os.path.exists( configFileName ):
    config = ConfigParser.ConfigParser()
    config.read( configFileName )
else:
    sys.stderr.write( "ERROR: Could not find Config file {c}!".format( c=configFileName) )
    sys.exit( -1 )

databaseDialect = config.get( 'DATABASE', 'database_dialect' )
databaseHost = config.get( 'DATABASE', 'database_host' )
databasePort = config.get( 'DATABASE', 'database_port' )
databaseName = config.get( 'DATABASE', 'database_name' )
databaseUsername = config.get( 'DATABASE', 'database_username' )
databasePassword = config.get( 'DATABASE', 'database_password' )
try:
    echo = config.getboolean( 'DATABASE', 'echo' )
except:
    echo = False
 

def get_engine( echo=False ):
    """! @brief get a database engine

    @param echo if True print SQL printing statements to stdout

    @return database engine
    """
    
    engine = sqlalchemy.create_engine( "{dialect}://{user}:{password}@{host}:{port}/{name}".format( dialect=databaseDialect,
                                                                                                    user=databaseUsername,
                                                                                                    password=databasePassword,
                                                                                                    host=databaseHost,
                                                                                                    port=databasePort,
                                                                                                    name=databaseName), 
                                       pool_size=10, # number of connections to keep open inside the connection pool
                                       max_overflow=100, # number of connections to allow in connection pool "overflow", that is connections that can be opened above and beyond the pool_size setting, which defaults to five.
                                       pool_recycle=3600, # this setting causes the pool to recycle connections after the given number of seconds has passed. 
                                       echo=echo )
    return engine

## @var engine
engine = get_engine( echo=echo )

## @var SessionFactory
# 
# http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html:
# Session is a regular Python class which can be directly instantiated. However, to standardize how sessions are 
# configured and acquired, the sessionmaker class is normally used to create a top level Session configuration 
# which can then be used throughout an application without the need to repeat the configurational arguments.
# sessionmaker() is a Session factory. A factory is just something that produces a new object when called.
#
SessionFactory = sqlalchemy.orm.sessionmaker( bind = engine )

## @var DBSession
# define a Session class which will serve as a factory for new Session objects
#
# A scoped_session is constructed by calling it, passing it a factory which can create new
# Session objects. A factory is just something that produces a new object when called, and in
# the case of Session
#
# See http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html#contextual-thread-local-sessions
# 
# If we call upon the registry DBSession a second time, we get back the same Session.

DBSession = sqlalchemy.orm.scoped_session( SessionFactory )


def init_db( e=None ):
    """! @brief Create all tables in the engine.

    @param e database engine
    
    This is equivalent to 'CREATE TABLE' statements in raw SQL.
    """
    
    Base.metadata.create_all( bind=e if e else engine )


