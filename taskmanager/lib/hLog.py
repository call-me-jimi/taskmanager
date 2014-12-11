import ConfigParser
import os

class hLog( object ):
    """! @brief raw implementation for configuring output of logger
    """
    def __init__( self, logger ):
        self.logger = logger

        self.logCategories = {}
        
        # load config file
        self.load()

    def load( self ):
        """! load config file about indication wether message of a particular category is passed to logger
        """
        
        # get path to taskmanager. it is assumed that this file is in the lib directory of
        # the taskmanager package.
        tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..') )

        configFileName = '{tmpath}/etc/logger.cfg'.format(tmpath=tmpath)
        parser = ConfigParser.SafeConfigParser()

        if os.path.exists( configFileName ):
            # read config file
            parser.read( configFileName )

            # remove all entries
            self.logCategories = {}

            # iterate over all categories
            for category in parser.items( 'CATEGORIES' ):
                try:
                    self.logCategories[ category[0] ] = True if category[1]=="True" else False
                except:
                    pass
        

    def write( self, msg, logCategory="default" ):
        if self.logCategories.get( logCategory, False ):
            self.logger.info( msg )

