import os
import ConfigParser

class hTaskDispatcherInfo(object):
    """! @brief Info about running TaskDispatcher"""
    def __init__(self):
        # get path to taskmanager. it is assumed that this file is in the lib directory of
        # the taskmanager package.
        self.tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..') )

        self.cfgFile = "%s/var/taskdispatcher.info" % self.tmpath
        self.taskDispatcherInfo = {}
        
        cfg = ConfigParser.SafeConfigParser()
        cfg.read( self.cfgFile )

        try:
            self.taskDispatcherInfo['host'] = cfg.get( 'SETTINGS', 'host' )
            self.taskDispatcherInfo['port'] = cfg.getint( 'SETTINGS', 'port' )
            self.taskDispatcherInfo['sslconnection'] = cfg.getboolean( 'SETTINGS', 'sslconnection' )
            self.taskDispatcherInfo['eocstring'] = cfg.get( 'SETTINGS', 'eocstring' )
        except: 
            #print "ERROR WHILE READING TD CONFIG FILE"
            pass

            
    def get( self, key, defaultValue=None):
        """! @brief get value for key
        
        @param key (string) A key given in .info file
        @param defaultValue Return defaultValue if key is not known
        """

        return self.taskDispatcherInfo.get( key, defaultValue )
        
    def save( self, settings):
        """! @brief Write settings into file

        @param settings (dict) Settings
        """
        # write host and port to file <varpath>/taskdispatcher.info
        cfg = ConfigParser.SafeConfigParser()

        cfg.add_section( 'SETTINGS' )

        for key,value in settings:
            cfg.set( 'SETTINGS', key, str(value) )

        with open( self.cfgFile, 'w') as f:
            cfg.write( f )
        
        
        
