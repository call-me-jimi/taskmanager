import os
from random import randrange

# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../..') )

libpath  = '%s/lib' % tmpath	# for hSocket

sys.path.insert(0,libpath)

from hTaskDispatcherInfo import hTaskDispatcherInfo
from hSocket import hSocket


class TMSConnector(object):
    """Class for establishing a running TMS"""
    def __init__(self,
                 sslConnection=False,
                 keyfile=None,
                 certfile=None,
                 ca_certs=None,
                 logFile = "",
                 binPath = None,
                 #serverPath = None,
                 vMode = False,
                 daemonMode = True,
                 persistent = False):
        self.sslConnection = sslConnection
        self.keyfile = keyfile
        self.certfile = certfile
        self.ca_certs = ca_certs
        self.logFi<leTMS = logFile
        self.binPath = binPath
        #self.serverPath = serverPath
        self.verboseMode = vMode
        self.clientSock = None
        self.running = False	# set true if tms responsed to check request
        self.started = False	# set true if tms has been started
        self.daemonMode = daemonMode
        self.persistent = persistent

        self.homedir = os.environ['HOME']

        # get stored tms info
        tmsInfo = hTaskDispatcherInfo()

        self.host = tdInfo.get('host', os.uname()[1] )
        self.port = tdInfo.get('port', randrange(10000,30000) )		# default port is between 10000 and 30000

        # pid of current process
        self.pid = os.getpid()


    def run(self):
        """ check if there is a TMS running on stored port. if not try to invoke one."""
        status = None
        cnt = 0

        # try maximal 10 times to invoke a TMS
        while cnt<10 and (status==None or status=="failed"):
            cnt += 1

            if self.verboseMode:
                print "[%s] [%s. attempt] checking TMS on %s:%s" % (self.pid, cnt, self.host, self.port)
                sys.stdout.flush()
                
            connStatus = self.connectToTMS()
            if self.verboseMode:
                if connStatus==1:
                    statusText = "TMS is running and understands me"
                elif connStatus==2:
                    statusText = "TMS is running but do not understands me"
                else:
                    statusText = "TMS is not running"

                print "[%s] [%s. attempt] ... status %s" % (self.pid,cnt,statusText)

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
                print "[%s] [%s. attempt] ... successful: %s" % (self.pid,cnt,invStatus)


        #if storeTMSProperties:
        #    self.storeTMSProperties()


    def connectToTMS(self):
        """ check for TMS
            return: 1 ... TMS is running and understands me
                    2 ... TMS is running but do not understands me
                    3 ... TMS is not running"""

        try:
            if self.verboseMode:
                print "[%s] ... connect to TMS on %s:%s ..." % (self.pid,self.host,self.port)

            job = "check"
            clientSock = hSocket(host=self.host,
                                 port=self.port,
                                 sslConnection=self.sslConnection,
                                 certfile=self.certfile,
                                 keyfile=self.keyfile,
                                 ca_certs=self.ca_certs,
                                 catchErrors=False)

            clientSock.send(job)
            response = clientSock.recv()

            if response == "ok":
                if self.verboseMode:
                    print "[%s] ... ... TMS is running." % (self.pid)

                self.running = True
                return 1
            else:
                if self.verboseMode:
                    print "[%s] ... ... TMS is running, but connection failed." % self.pid

                self.running = False
                return 2

        except socket.error,msg:
            self.running = False
            if self.verboseMode:
                print "[%s] ... ... TMS is not running." % self.pid

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
            daemonModeStr = ""
            vTMSMode = ""
            if not self.daemonMode:
                vTMSMode = "-v"
                daemonModeStr = "-d"
                if self.persistent:
                    daemonModeStr += " -s"

            if self.logFileTMS:
                com = "%s/TMS %s %s -l %s %s" % (self.binPath,
                                                 daemonModeStr,
                                                 vTMSMode,
                                                 self.logFileTMS,
                                                 self.port)
            else:
                com = "%s/TMS %s %s %s" % (self.binPath,
                                           daemonModeStr,
                                           vTMSMode,
                                           self.port)
            if self.verboseMode:
                print "[%s] ... command: %s" % (self.pid,com)

            if not self.daemonMode:
                # invoke TMS and wait until it has finished
                sp=subprocess.Popen([com],
                                    shell=True,
                                    stdout=subprocess.PIPE)

                while True:
                    o = sp.stdout.readline()
                    if o == "" and sp.poll()!= None:
                        break

                    sys.stdout.write(o)

                sys.stdout.write("Exit.\n")
                sys.stdout.flush()

                return "finished"
            else:
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

