# create a Session object by sessionmaker

import os
import ConfigParser
import sqlalchemy.orm

# get path to taskmanager. it is assumed that this script is in the lib directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..' ) )

etcpath    = '%s/etc'    % tmpath	# for configuration files

# library is in the name folder
from hDatabase import Base

# read config file
configFileName = "%s/serversettings.cfg" % etcpath
if os.path.exists( configFileName ):
    config = ConfigParser.ConfigParser()
    config.read( configFileName )
else:
    sys.stderr.write( "ERROR: Could not find Config file {c}!".format( c=configFileName) )
    sys.exit( -1 )

databaseDialect = config.get( 'DATABASE', 'database_dialect' )
databaseName = config.get( 'DATABASE', 'database_name' )
databaseUsername = config.get( 'DATABASE', 'database_username' )
databasePassword = config.get( 'DATABASE', 'database_password' )
databaseHost = config.get( 'DATABASE', 'database_host' )



## @var engine                                                                                                                                               
#The engine that is connected to the database                                                                                                                         
#use "echo=True" for SQL printing statements to stdout                                                                                                                
engine = sqlalchemy.create_engine( "{dialect}://{user}:{password}@{host}/{name}".format( dialect=databaseDialect,
                                                                                         user=databaseUsername,
                                                                                         password=databasePassword,
                                                                                         host=databaseHost,
                                                                                         name=databaseName), echo=False )

# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all( engine )

## @var DBsession
# define a Session class which will serve as a factory for new Session objects
# 
# http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html:
# Session is a regular Python class which can be directly instantiated. However, to standardize how sessions are 
# configured and acquired, the sessionmaker class is normally used to create a top level Session configuration 
# which can then be used throughout an application without the need to repeat the configurational arguments.
#
# Thread local factory for sessions. See http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html#contextual-thread-local-sessions                                       
DBSession = sqlalchemy.orm.sessionmaker( bind = engine )








