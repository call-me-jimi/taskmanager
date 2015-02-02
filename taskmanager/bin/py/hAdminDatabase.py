#!/usr/bin/env python

PROGNAME = "hAdminDatabase"

import sys
import os
import argparse
import textwrap
import sqlalchemy
import ConfigParser

# logging
import logging
logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.ERROR)			# logger level. can be changed with command line option -v

formatter = logging.Formatter('[%(asctime)-15s] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.INFO)		# handler level. 
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)


# get path to config file. it is assumed that this progran is in the bin/py directory of
# the package hierarchy.
etcpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../../etc' ) )
libpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../../lib' ) )

sys.path.insert(0,libpath)

# default config file for database connection
defaultConfigFileName = "{etcPath}/serversettings.cfg".format(etcPath=etcpath)

class ValidateVerboseMode(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        #print '{n} -- {v} -- {o}'.format(n=namespace, v=value, o=option_string)

        # set level of logger to INFO
        logger.setLevel( logging.INFO )
        
        # set attribute self.dest
        setattr(namespace, self.dest, True)
        
if __name__ == '__main__':
    textWidth = 80
    parser = argparse.ArgumentParser(
        prog=PROGNAME,
        usage="%(prog)s [-h --help] [options]",
        description="Connect to a databaseb",
        epilog='Written by Hendrik.' )

    parser.add_argument('-c', '--config_file',
                        nargs = 1,
                        metavar = "FILE",
                        dest = 'configFileName',
                        default = defaultConfigFileName,
                        help = 'Read a different config file. Default: {f}'.format(f=defaultConfigFileName)
                        )

    parser.add_argument('-C', '--create_tables',
                        dest = 'createTables',
                        action = 'store_true',
                        default = False,
                        help = 'Create all tables in database.'
                        )

    parser.add_argument('-D', '--drop_tables',
                        dest = 'dropTables',
                        action = 'store_true',
                        default = False,
                        help = 'Drop all tables in database.'
                        )

    parser.add_argument('-s', '--show_database_configuration',
                        dest = 'showDatabaseConfig',
                        action = 'store_true',
                        default = False,
                        help = 'Show database configuration.'
                        )
    
    parser.add_argument('-v', '--verbose_mode',
                        nargs = 0,
                        dest = 'verboseMode',
                        action = ValidateVerboseMode,
                        default = False,
                        help = 'Activate verbose mode.'
                        )
    
    args = parser.parse_args()


    logger.info( "Welcome to {p}!".format(p=PROGNAME) )

    if args.showDatabaseConfig:
        logger.info( "Read config file {f}".format(f=args.configFileName) )
        # read config file
        # read config file
        configFileName = "%s/serversettings.cfg" % etcpath
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

        print "Database configuration:"
        print
        print "{k:>20} : {v}".format( k='database_dialect', v=databaseDialect )
        print "{k:>20} : {v}".format( k='database_host', v=databaseHost )
        print "{k:>20} : {v}".format( k='database_port', v=databasePort )
        print "{k:>20} : {v}".format( k='database_name', v=databaseName )
        print "{k:>20} : {v}".format( k='database_username', v=databaseUsername )
        print "{k:>20} : {v}".format( k='database_password', v=databasePassword )

    elif args.createTables:
        # This will not re-create tables that already exist.

        logger.info( "Create tables in database" )

        from hDBSessionMaker import hDBSessionMaker
        
        dbSessionMaker = hDBSessionMaker( configFileName=args.configFileName,
                                          createTables=True,
                                          echo=True )
        
        logger.info( "done." )

    elif args.dropTables:
        # This will really drop all tables including their contents.

        answer = raw_input( "Are you sure you want to drop all tables [y|N]?" )
        if answer=='y':
            logger.info( "Drop tables in database" )
            
            from hDBConnection import hDBConnection
            
            session = hDBConnection()
            
            session.drop_all_tables()
            
            logger.info( "done." )
        else:
            logger.info( "Nothing has been done." )

    logger.info( "Thank you for using {p}!".format( p=PROGNAME ) )




