import os
import sys
import traceback
from random import randrange
import socket
import subprocess
import re
import ConfigParser

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

        # read default config file
        defaultConfigFileName = "%s/serversettings.cfg" % etcPath
        if os.path.exists( defaultConfigFileName ):
            defaultConfig = ConfigParser.ConfigParser()
            defaultConfig.read( defaultConfigFileName )
        else:
            print "ERROR: config file {f} could not be found".format( f="%s/serversettings.cfg" % etcPath )

        useSSLConnection = defaultConfig.getboolean( 'CONNECTION', 'sslConnection' )
        EOCString = defaultConfig.get( 'CONNECTION', 'EOCString' )
        
        if self.serverType=='TMS':
            # get stored tms info
            tmsInfo = hTaskManagerServerInfo()

            self.host = tmsInfo.get('host')
            self.port = tmsInfo.get('port', getDefaultTMSPort( self.user ) )
            self.sslConnection = tmsInfo.get('sslConnection', useSSLConnection)
            self.EOCString = tmsInfo.get('eocstring', EOCString)
        elif self.serverType=='TMMS':
            # get stored tmms info
            tmmsInfo = hTaskManagerMenialServerInfo( self.host )

            self.port = tmmsInfo.get('port', getDefaultTMMSPort( self.user ))
            self.sslConnection = tmmsInfo.get('sslConnection', useSSLConnection)
            self.EOCString = tmmsInfo.get('eocstring', EOCString)
        elif self.serverType != '':
            print "ERROR: server type is not unknown!"
        else:
            print "ERROR: server type is missing!"

        # pid of current process
        self.pid = os.getpid()


    def run(self):
        """ check if there is a server running on stored port. if not try to invoke one."""
        status = None
        cnt = 0

        # try maximal 5 times to invoke a server
        while cnt<5 and not self.running:
            cnt += 1

            if self.verboseMode:
                print "[%s] [%s. attempt] checking server on %s:%s" % (self.pid, cnt, self.host, self.port)
                sys.stdout.flush()

            connStatus = self.connectToServer( cnt )
            #if self.verboseMode:
            #    if connStatus==1:
            #        statusText = "Server is running and understands me"
            #    elif connStatus==2:
            #        statusText = "Server is running but do not understands me"
            #    else:
            #        statusText = "Server is not running"
            #
            #    print "[%s] [%s. attempt] ... status %s" % (self.pid,cnt,statusText)

            if connStatus == 1:
                # Server is running and understands me
                break
            elif connStatus == 2:
                # Server is running but do not understands me
                self.port += 1
            elif connStatus == 3:
                # Server is not running
                newHost = os.uname()[1]

                # store new host
                if newHost != self.host:
                    self.host = newHost

            # try to start a new Server on port in case of status 2 or 3
            if self.verboseMode:
                print "[%s] [%s. attempt] invoke Server ..." % (self.pid,cnt)
                
            status = self.invokeServer( cnt )




    def connectToServer(self, cnt=1):
        """ check for Server
            return: 1 ... Server is running and understands me
                    2 ... Server is running but do not understands me
                    3 ... Server is not running"""

        try:
            if self.verboseMode:
                print "[%s] [%s. attempt] ... connecting to Server on %s:%s ..." % (self.pid,cnt,self.host,self.port)
                sys.stdout.flush()

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
                if self.verboseMode:
                    print "[%s] [%s. attempt] ... is running." % (self.pid,cnt)

                self.running = True
                return 1
            else:
                if self.verboseMode:
                    print "   ",response
                    print "[%s] [%s. attempt] ... is running, but connection failed." % (self.pid,cnt)

                self.running = False
                return 2

        except socket.error,msg:
            # msg is something like "[Errno 111] Connection refused":
            self.running = False
            if self.verboseMode:
                print "[%s] [%s. attempt] ... is not running" % (self.pid,cnt)

            return 3
            
        except AttributeError,msg:
            self.running = False
            if self.verboseMode:
                print "[%s] [%s. attempt] ... is not running" % (self.pid,cnt)

            return 3
            


    def invokeServer(self, cnt):
        """@brief  invoke server on given host and port

        @return (successful|finished) successful: started server, failed: start has failed
        """

        if self.verboseMode:
            print "[%s] [%s. attempt] ... try to start server on %s:%s ..." % (self.pid,cnt,self.host,self.port)


        try:
            if self.serverType=='TMS':
                run = 'hRunTMS.py'
            elif self.serverType=='TMMS':
                run = 'hRunTMMS.py'
            
            com = "python {binpath}/python/{run} -p {port}".format( run = run,
                                                                    binpath = binPath,
                                                                    port = self.port)

            if self.verboseMode:
                print "[%s] [%s. attempt] ... command: %s" % (self.pid,cnt,com)

            # invoke server as daemon
            sp=subprocess.Popen(com,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
            out,err = sp.communicate()

            if self.verboseMode and out:
                print out

            if re.search('Address already in use',err):
                print "[%s] [%s. attempt] ... ... Address already in use" % self.pid
                self.started = False
                self.running = False
                return "failed"
            elif err:
                print "[%s] [%s. attempt] ... ... Server error while initiation: %s" % (self.pid,cnt,err)
                self.started = False
                self.running = False
                return "failed"
            else:
                self.started = True
                if self.verboseMode:
                    print "[%s] [%s. attempt] ... ... Server has been started." % (self.pid,cnt)

                return "successful"

        except socket.error,msg:
            sys.sterr.write("Connection to %s:%s could not be established\n" % (host,port))
            return "failed"


    def send(self,command, createNewSocket=False):
        if self.verboseMode:
            print "... send request: %s" % command

        if createNewSocket:
            self.clientSock = hSocket(host=self.host,
                                      port=self.port,
                                      EOCString=self.EOCString,
                                      sslConnection=self.sslConnection,
                                      certfile=self.certfile,
                                      keyfile=self.keyfile,
                                      ca_certs=self.ca_certs,
                                      catchErrors=False)

        self.clientSock.send(command)

    def recv(self):
        recv = self.clientSock.recv()

        if self.verboseMode:
            print "... received response from server:",recv

        return recv

    def close(self):
        """ close connection """
        #self.clientSock.close()
        self.clientSock.shutdown(socket.SHUT_RDWR)
        self.openConnection = False

    def sendAndRecv(self,request):
        """ send request to server and receive response"""
        self.send(request)

        return self.recv()

    def sendAndClose(self,request):
        """ send request to server and close connection"""
        try:
            self.send(request)
            self.close()
        except socket.error,msg:
            self.openConnection = False
            sys.stderr.write("SOCKET ERROR: % s\n" % msg)
        except:
            self.openConnection = False
            sys.stderr.write("UNKNOWN ERROR: % s\n" % sys.exc_info()[0])
            traceback.print_exc(file=sys.stderr)


    def sendAndRecvAndClose(self,request):
        """ send request to server, receive response and close connection"""
        try:
            self.send(request)
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
