import os
import ConfigParser

class hTaskDispatcherInfo(object)
    def __init__(self):
        # get path to taskmanager. it is assumed that this file is in the lib directory of
        # the taskmanager package.
        self.tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..') )
        
        self.taskDispatcherInfo = {}
        if os.path.exists("%s/taskdispatcher.info" % self.tmpath):
            cfg = ConfigParser.SafeConfigParser()
            cfg.read( "%s/taskdispatcher.info" % self.tmpath )
            
            try:
                self.taskDispatcherInfo['host'] = cfg.get( 'SETTINGS', 'host' )
                self.taskDispatcherInfo['port'] = cfg.getint( 'SETTINGS', 'port' )
                self.taskDispatcherInfo['sslconnection'] = cfg.getbool( 'SETTINGS', 'sslconnection' )
            except: 
                print "ERROR WHILE READING TD CONFIG FILE"
                
    def get( self, key, defaultValue=None):
        """! @brief get value for key
        
        @param key (string) A key given in .info file
        @param defaultValue Return defaultValue if key is not known
        """
        
        return self.taskDispatcherInfo.get( key, defaultValue )
        
