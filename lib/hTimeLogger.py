
import sys
from datetime import datetime
import logging

# create logger and configure
logger = logging.getLogger('TimeLogger')
logger.propagate = False
logger.setLevel(logging.DEBUG)

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.DEBUG)

# add handler to logger
logger.addHandler(consoleLog)



class hTimeLogger( object ):
    """! brief An Object which handles time logging"""
    globalLoggerLevel = 10
    def __init__(self, prefix="", indentation=0, loggerLevel=10):
        """! @brief constructor

        @param prefix (string) prefix will be written in front of each log
        @param indentation (int) indentation in front of each log
        @param loggerLevel (int) level of logging
        """
        
        self.initTime = datetime.now()

        if prefix:
            self.prefix = "{1}[{0}] ".format( prefix, " "*indentation )

        self.loggerLevel = loggerLevel
        self.setLoggingLevel( self.globalLoggerLevel )
        
        logger.log( self.loggerLevel, "{0}[{1}] Start logging ...".format(self.prefix, str(self.initTime)) )
        self.stopped = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """! @brief execution after context has been left"""
        
        if not self.stopped:
            self.stopLogging()

    def setLoggingLevel( self, val ):
        """! @brief set logging level of logger to val"""
        logger.setLevel( val )
        
    def log(self, text):
        """! @brief Print out text along with time """
        logger.log( self.loggerLevel, "{0}   [{1}] {2}".format( self.prefix, str(datetime.now()-self.initTime), text ) )

    def stopLogging(self):
        """! @brief stop logging"""
        try:
            self.stopped = True
            logger.log( self.loggerLevel, "{0}[{1}] Stop logging after {2} ...".format(self.prefix, str(datetime.now()), str(datetime.now()-self.initTime)) )
        except:
            pass
        
    def __del__(self):
        """! @brief Desctructor - last print out"""
        if not self.stopped:
            self.stopLogging()
    
