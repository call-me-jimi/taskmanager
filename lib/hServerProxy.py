import os
import sys
import traceback
from random import randrange
import socket
import subprocess
import re
import ConfigParser
import socket

# logging
import sys
import logging
logger = logging.getLogger('hServerProxy')
logger.setLevel(logging.WARNING)
logger.propagate = False

formatter = logging.Formatter('[%(asctime)-15s] [hServerProxy] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.DEBUG)
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)



# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..') )

etcPath = '%s/etc' % tmpath   # for hSocket
binPath = '%s/bin' % tmpath	  # for hRunTMS

#sys.path.insert(0,libPath)

from hSocket import hSocket
from hTaskManagerServerInfo import hTaskManagerServerInfo
from hTaskManagerMenialServerInfo import hTaskManagerMenialServerInfo
from hTMUtils import getDefaultTMSPort, getDefaultTMMSPort

class hServerProxy(object):
    """! @brief Class for establishing a running Server, such as TMS or TMMS and connect to it"""
    def __init__(self,
                 user,
                 serverType='',
                 host=None,
                 sslConnection=False,
                 keyfile=None,
                 certfile=None,
                 ca_certs=None,
                 logFile = "",
                 verboseMode = False,
                 persistent = False):
        """! @brief Constructor
        
        @param serverType (string) Type of server. Currently supported are TMS and TMMS.
        
        """
        self.user = user
        self.serverType = serverType
        self.host = host
        self.sslConnection = sslConnection
        self.keyfile = keyfile
        self.certfile = certfile
        self.ca_certs = ca_certs
        self.logFile = logFile
        self.verboseMode = verboseMode
        self.clientSock = None
        self.running = False	# set true if server responsed to check request
        self.started = False	# set true if server has been started
        self.persistent = persistent

        self.homedir = os.environ['HOME']

        if verboseMode:
            logger.setLevel( logging.DEBUG )

        logger.info( 'server proxy for {s}'.format(s=self.serverType) )
        
        # read default config file
        defaultConfigFileName = "%s/serversettings.cfg" % etcPath
        
        logger.info( "read config file {f}".format(f=defaultConfigFileName) )
        
        if os.path.exists( defaultConfigFileName ):
            defaultConfig = ConfigParser.ConfigParser()
            defaultConfig.read( defaultConfigFileName )
        else:
            logger.info( "ERROR: config file {f} could not be found".format( f=defaultConfigFileName ) )

        # python executable (to start server)
        self.python = defaultConfig.get( 'SYSTEM', 'python' )
        
        useSSLConnection = defaultConfig.getboolean( 'CONNECTION', 'sslConnection' )
        EOCString = defaultConfig.get( 'CONNECTION', 'EOCString' )
        
        if self.serverType=='TMS':
            # get stored tms info
            tmsInfo = hTaskManagerServerInfo()
            logger.info( "read config file {f} for TMS.".format( f=tmsInfo.configFile ) )
            
            self.host = tmsInfo.get('host', os.uname()[1])
            self.port = tmsInfo.get('port', getDefaultTMSPort( self.user ) )
            self.sslConnection = tmsInfo.get('sslConnection', useSSLConnection)
            self.EOCString = tmsInfo.get('eocstring', EOCString)
        elif self.serverType=='TMMS':
            # get stored tmms info
            tmmsInfo = hTaskManagerMenialServerInfo( self.host )
            logger.info( "read config file {f} for TMMS.".format( f=tmmsInfo.configFile ) )

            self.port = tmmsInfo.get('port', getDefaultTMMSPort( self.user ))
            self.sslConnection = tmmsInfo.get('sslConnection', useSSLConnection)
            self.EOCString = tmmsInfo.get('eocstring', EOCString)
        elif self.serverType != '':
            logger.warning( "ERROR: server type is not unknown!" )
        else:
            logger.warning( "ERROR: server type is missing!" )

        # pid of current process
        self.pid = os.getpid()

        #logger.info( 'pid {p}'.format(p=self.pid) )

    def run(self):
        """! @brief check if there is a server running on stored port. if not try to invoke one."""
        status = None
        cnt = 0

        logger.info( 'run server' )
        
        # try maximal 5 times to invoke a server
        while cnt<5 and not self.running:
            cnt += 1

            logger.info( "[{i}. attempt] checking server on {h}:{p}. ".format(i=cnt, h=self.host, p=self.port) )

            connStatus = self.connectToServer( cnt )

            if connStatus == 1:
                # Server is running and understands me
                break
            elif connStatus == 2:
                # Server is running but do not understands me
                self.port += 1
            elif connStatus == 3:
                # Server is not running
                ##newHost = os.uname()[1]
                ##
                ### store new host
                ##if newHost != self.host:
                ##    self.host = newHost
                pass

            # try to start a new Server on port in case of status 2 or 3
            logger.info( "[{i}. attempt] invoke server on {h}:{p}.".format(i=cnt, h=self.host, p=self.port) )
                
            status = self.invokeServer( cnt )

    def isRunning( self ):
        """! @brief check if tmms is running

        @return (boolean) True|False
        """

        connStatus = self.connectToServer( 1 )

        if connStatus == 1:
            # Server is running and understands me
            return True
        elif connStatus == 2:
            # Server is running but do not understands me
            return False
        elif connStatus == 3:
            # Server is not running
            return False
        

    def connectToServer(self, cnt=1):
        """ check for Server
            return: 1 ... Server is running and understands me
                    2 ... Server is running but do not understands me
                    3 ... Server is not running"""

        try:
            logger.info( "[{i}. attempt] connecting to server on {h}:{p}.".format(i=cnt, h=self.host, p=self.port) )

            command = "ping"
            self.clientSock = hSocket(host=self.host,
                                      port=self.port,
                                      EOCString=self.EOCString,
                                      sslConnection=self.sslConnection,
                                      certfile=self.certfile,
                                      keyfile=self.keyfile,
                                      ca_certs=self.ca_certs,
                                      catchErrors=False)

            self.clientSock.send(command)
            
            response = self.clientSock.recv()
            
            if response == "pong":
                logger.info( "[{i}. attempt] ... server on {h}:{p} is running.".format(i=cnt, h=self.host, p=self.port) )

                self.running = True
                return 1
            else:
                logger.info( "[{i}. attempt] ... server on {h}:{p} is running, but connection failed.".format(i=cnt, h=self.host, p=self.port) )
                logger.info( "[{i}. attempt] ... error: {e}".format(i=cnt, e=response) )

                self.running = False
                return 2

        except socket.error,msg:
            # msg is something like "[Errno 111] Connection refused":
            self.running = False

            logger.info( "[{i}. attempt] ... server on {h}:{p} is NOT running.".format(i=cnt, h=self.host, p=self.port) )
            logger.info( "[{i}. attempt] ... error: {e}".format(i=cnt, e=msg) )

            return 3
            
        except AttributeError,msg:
            self.running = False
            
            logger.info( "[{i}. attempt] ... server on {h}:{p} is NOT running.".format(i=cnt, h=self.host, p=self.port) )
            logger.info( "[{i}. attempt] ... error: {e}".format(i=cnt, e=msg) )

            return 3
            


    def invokeServer(self, cnt):
        """@brief  invoke server on given host and port

        @return (successful|finished) successful: started server, failed: start has failed
        """

        logger.info( "[{i}. attempt] invoke server on {h}:{p}. ".format(i=cnt, h=self.host, p=self.port) )

        try:
            if self.serverType=='TMS':
                runServer = 'hRunTMS.py'
            elif self.serverType=='TMMS':
                runServer = 'hRunTMMS.py'

            com = "ssh -x -a {host} {python} {binpath}/python/{runServer} -p {port}".format( python = self.python,
                                                                                             host = self.host,
                                                                                             runServer = runServer,
                                                                                             binpath = binPath,
                                                                                             port = self.port)

            logger.info( "[{i}. attempt] command:".format(i=cnt, command=com) )

            # invoke server as daemon
            sp=subprocess.Popen(com,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
            out,err = sp.communicate()

            if re.search('Address already in use',err):
                logger.info( "[{i}. attempt] ... Address already in use".format(i=cnt) )
                
                self.started = False
                self.running = False
                
                return "failed"
            elif err:
                logger.info( "[{i}. attempt] ... Server error while initiation: {err}".format(i=cnt, err=err) )
                
                self.started = False
                self.running = False
                
                return "failed"
            else:
                self.started = True

                logger.info( "[{i}. attempt] ... Server has been started on {h}:{p}".format(i=cnt, h=self.host, p=self.port) )

                return "successful"

        except socket.error,msg:
            logger.warning( "[{i}. attempt] ... Connection to server on {h}:{p} could not be established".format(i=cnt, h=self.host, p=self.port) )
            
            return "failed"


    def send(self,command, createNewSocket=False):
        logger.info( "send request: {c}".format(c=command) )

        if createNewSocket:
            logger.info( "create new socket" )
            
            try:
                self.clientSock = hSocket(host=self.host,
                                          port=self.port,
                                          EOCString=self.EOCString,
                                          sslConnection=self.sslConnection,
                                          certfile=self.certfile,
                                          keyfile=self.keyfile,
                                          ca_certs=self.ca_certs,
                                          catchErrors=False)
            except socket.error, msg:
                return msg

        self.clientSock.send(command)

        logger.info( "... done" )
        
        return True

    def recv(self):
        recv = self.clientSock.recv()

        recvShort = recv.replace('\n', '\\')[:30]
        logger.info( "response from server: {r}{dots}".format(r=recvShort, dots="..." if len(recv)>30 else "" ) )

        return recv

    def close(self):
        """ close connection """
        #self.clientSock.close()

        logger.info( "close server" )
        
        self.clientSock.shutdown(socket.SHUT_RDWR)
        self.openConnection = False

    def sendAndRecv(self,request, createNewSocket=False):
        """ send request to server and receive response"""
        self.send(request, createNewSocket)

        return self.recv()

    def sendAndClose(self,request, createNewSocket=False):
        """ send request to server and close connection"""
        try:
            self.send(request, createNewSocket)
            self.close()
        except socket.error,msg:
            self.openConnection = False
            sys.stderr.write("SOCKET ERROR: % s\n" % msg)
        except:
            self.openConnection = False
            sys.stderr.write("UNKNOWN ERROR: % s\n" % sys.exc_info()[0])
            traceback.print_exc(file=sys.stderr)


    def sendAndRecvAndClose(self,request, createNewSocket=False):
        """ send request to server, receive response and close connection"""
        try:
            self.send(request, createNewSocket)
            response = self.recv()
            self.close()
            return response
        except socket.error,msg:
            self.openConnection = False
            sys.stderr.write("SOCKET ERROR: % s\n" % msg)
        except:
            self.openConnection = False
            sys.stderr.write("UNKNOWN ERROR: % s\n" % sys.exc_info()[0])
            traceback.print_exc(file=sys.stderr)

    def shutdown( self ):
        """! brief shutdown server """

        command = "shutdown"
        self.clientSock = hSocket(host=self.host,
                                  port=self.port,
                                  EOCString=self.EOCString,
                                  sslConnection=self.sslConnection,
                                  certfile=self.certfile,
                                  keyfile=self.keyfile,
                                  ca_certs=self.ca_certs,
                                  catchErrors=False)
        
        self.clientSock.send(command)

        return "done"
            
        
