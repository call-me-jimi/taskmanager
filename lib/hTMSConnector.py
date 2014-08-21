import os
import sys
import traceback
from random import randrange
import socket
import subprocess
import re

# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..') )

#libPath  = '%s/lib' % tmpath	# for hSocket
#binPath = '%s/bin' % tmpath	# for hRunTMS

#sys.path.insert(0,libPath)

from hTaskManagerServerInfo import hTaskManagerServerInfo
from hSocket import hSocket


class hTMSConnector(object):
    """Class for establishing a running TMS and connect to it"""
    def __init__(self,
                 sslConnection=False,
                 keyfile=None,
                 certfile=None,
                 ca_certs=None,
                 logFile = "",
                 verboseMode = False,
                 persistent = False):
        self.sslConnection = sslConnection
        self.keyfile = keyfile
        self.certfile = certfile
        self.ca_certs = ca_certs
        self.logFileTMS = logFile
        self.verboseMode = verboseMode
        self.clientSock = None
        self.running = False	# set true if tms responsed to check request
        self.started = False	# set true if tms has been started
        self.persistent = persistent

        self.homedir = os.environ['HOME']

        # get stored tms info
        tmsInfo = hTaskManagerServerInfo()

        self.host = tmsInfo.get('host')
        self.port = tmsInfo.get('port')
        self.EOCString = tmsInfo.get('eocstring')

        # pid of current process
        self.pid = os.getpid()


    def run(self):
        """ check if there is a TMS running on stored port. if not try to invoke one."""
        status = None
        cnt = 0

        # try maximal 5 times to invoke a TMS
        while cnt<5 and (status==None or status=="failed"):
            cnt += 1

            if self.verboseMode:
                print "[%s] [%s. attempt] checking TMS on %s:%s" % (self.pid, cnt, self.host, self.port)
                sys.stdout.flush()

            connStatus = self.connectToTMS()
            #if self.verboseMode:
            #    if connStatus==1:
            #        statusText = "TMS is running and understands me"
            #    elif connStatus==2:
            #        statusText = "TMS is running but do not understands me"
            #    else:
            #        statusText = "TMS is not running"
            #
            #    print "[%s] [%s. attempt] ... status %s" % (self.pid,cnt,statusText)

            if connStatus == 1:
                # TMS is running and understands me
                break
            elif connStatus == 2:
                # TMS is running but do not understands me
                self.port += 1
            elif connStatus == 3:
                # TMS is not running
                newHost = os.uname()[1]

                # store new host
                if newHost != self.host:
                    self.host = newHost

            # try to start a new TMS on port in case of status 2 or 3
            if self.verboseMode:
                print "[%s] [%s. attempt] invoke TMS ..." % (self.pid,cnt)
                
            invStatus = self.invokeTMS()

            if invStatus == "finished":
                break

            if self.verboseMode:
                print "[%s] [%s. attempt] ... %s" % (self.pid,cnt,invStatus)


        #if storeTMSProperties:
        #    self.storeTMSProperties()


    def connectToTMS(self):
        """ check for TMS
            return: 1 ... TMS is running and understands me
                    2 ... TMS is running but do not understands me
                    3 ... TMS is not running"""

        try:
            if self.verboseMode:
                print "[%s] ... connect to TMS on %s:%s ..." % (self.pid,self.host,self.port),
                sys.stdout.flush()

            command = "check"
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

            if response == "ok":
                if self.verboseMode:
                    print "is running."

                self.running = True
                return 1
            else:
                if self.verboseMode:
                    print "is running, but connection failed."

                self.running = False
                return 2

        except socket.error,msg:
            # msg is something like "[Errno 111] Connection refused":
            self.running = False
            if self.verboseMode:
                print "is not running."

            return 3


    def invokeTMS(self,storeConfig=False):
        """@brief  invoke TMS on given host and port

        @param storeConfig store configuration in file

        @return "successful": successful start of TMS, "finished": TMS has been started and terminated, "failed": start has failed
        """

        #if storeConfig:
        #    self.storeTMSProperties()

        if self.verboseMode:
            print "[%s] ... try to start TMS on %s:%s ..." % (self.pid,self.host,self.port)


        try:
            com = "python {binpath}/python/hRunTMS.py -p {port}".format( binpath = binPath,
                                                                  port = self.port)
            
            if self.verboseMode:
                print "[%s] ... command: %s" % (self.pid,com)

            # invoke TMS as daemon
            sp=subprocess.Popen(com,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
            out,err = sp.communicate()

            if self.verboseMode:
                print out

            if re.search('Address already in use',err):
                print "[%s] ... ... Address already in use" % self.pid
                return "failed"
            elif err:
                print "[%s] ... ... TMS Error while initiation: %s" % (self.pid,err)
                return "failed"
            else:
                self.started = True
                if self.verboseMode:
                    print "[%s] ... ... TMS has been started." % self.pid

                return "successful"

        except socket.error,msg:
            sys.sterr.write("Connection to %s:%s could not be established\n" % (host,port))
            return "failed"


    def send(self,command):
        if self.verboseMode:
            print "... send request: %s" % command

        self.clientSock.send(command)

    def recv(self):
        recv = self.clientSock.recv()

        if self.verboseMode:
            print "... received response from TMS:",recv

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
