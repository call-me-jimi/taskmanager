import os
import ConfigParser
import traceback
import sys

homedir = os.environ['HOME']
BASEDIR =  "{home}/.taskmanager".format(home=homedir)

class hTaskManagerMenialServerInfo(object):
    """! @brief Info about running TaskManagerMenialServer"""
    def __init__(self, host):
        homedir = os.environ['HOME']

        # read host and port of running or last TMMS
        self.configFile = "{basedir}/tmms.{host}.info".format(basedir=BASEDIR,host=host)

        self.tmmsInfo = {}
        
        cfg = ConfigParser.SafeConfigParser()
        cfg.read( self.configFile )

        try:
            self.tmmsInfo['host'] = cfg.get( 'SETTINGS', 'host' )
            self.tmmsInfo['port'] = cfg.getint( 'SETTINGS', 'port' )
            self.tmmsInfo['sslconnection'] = cfg.getboolean( 'SETTINGS', 'sslconnection' )
            self.tmmsInfo['eocstring'] = cfg.get( 'SETTINGS', 'eocstring' )
        except: 
            #traceback.print_exc(file=sys.stdout)
            #print "ERROR WHILE READING TD CONFIG FILE"
            pass

            
    def get( self, key, defaultValue=None):
        """! @brief get value for key
        
        @param key (string) A key given in .info file
        @param defaultValue Return defaultValue if key is not known
        """

        return self.tmmsInfo.get( key, defaultValue )
        
    def save( self, settings):
        """! @brief Write settings into file

        @param settings (dict) Settings
        """
        # create .taskmanager directory
        if not os.path.exists( BASEDIR ):
            os.mkdir( BASEDIR )
        
        # write host and port to file <varpath>/taskdispatcher.info
        cfg = ConfigParser.SafeConfigParser()

        cfg.add_section( 'SETTINGS' )

        for key,value in settings:
            cfg.set( 'SETTINGS', key, str(value) )

        with open( self.configFile,'w') as f:
            cfg.write( f )
        
        
        
