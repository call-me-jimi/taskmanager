# user specific task manager server (TMS) which is usually started on start by hRunJob as daemon

# logging
import sys
import logging
logger = logging.getLogger('TMS')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)-15s] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.INFO)
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)


from time import time, strftime,sleep
from datetime import datetime

invokingTMSTime1 = datetime.now()

#import socket
import SocketServer
from threading import Thread,Lock
import threading
import select
import re
import subprocess
import pwd
from os import system,uname,environ,getlogin
import getopt
from random import choice
from string import join,replace
import pickle
import string
from copy import copy,deepcopy
import os
import traceback
import exceptions
import json
import textwrap
from sqlalchemy import and_, not_, func
from sqlalchemy.orm.exc import NoResultFound

homedir = os.environ['HOME']
user = pwd.getpwuid(os.getuid())[0]

# get path to taskmanager. it is assumed that this script is in the lib directory of
# the taskmanager package.
#tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..' ) )

# set several paths
#binPath    = '%s/bin' % tmpath		# for TMMS
#serverPath = '%s/UserServer' % tmpath
#etcPath    = '%s/etc'    % tmpath	# for TaskDispatcher.info
#libPath  = '%s/lib' % tmpath		# for hSocket

# ssl configuration
certfile = "%s/.taskmanager/%s.crt" % (homedir,user)
keyfile = "%s/.taskmanager/%s.key" % (homedir,user)
ca_certs = "%s/.taskmanager/ca_certs.%s.crt" % (homedir,user)
 

from hSocket import hSocket

from hTaskDispatcherInfo import hTaskDispatcherInfo
from hTaskManagerServerInfo import hTaskManagerServerInfo
from hTMUtils import renderHelp
from daemon import Daemon
from hCommand import hCommand
from hDBConnection import hDBConnection
from hServerProxy import hServerProxy
import hDatabase as db

#history = hJobHistory(100000)


# get stored host and port from taskdispatcher
tdInfo = hTaskDispatcherInfo()

TaskDispatcherHost = tdInfo.get('host', None)
TaskDispatcherPort = tdInfo.get('port', None)



# TaskManagerServer is a TCPServer using the threading mix in to create a new thread for every request.
class TaskManagerServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer, Daemon):
    # This means the main server will not do the equivalent of a
    # pthread_join() on the new threads.  With this set, Ctrl-C will
    # kill the server reliably.
    daemon_threads = True

    # By setting this it is allowed for the server to re-bind to the address by
    # setting SO_REUSEADDR, meaning you don't have to wait for
    # timeouts when you kill the server and the sockets don't get
    # closed down correctly.
    allow_reuse_address = True
    reuest_queue_size = 10

    def __init__(self,
                 port,
                 handler,
                 processor,
                 EOCString=None,
                 sslConnection=False,
                 keyfile=None,
                 certfile=None,
                 ca_certs=None,
                 verboseMode=False,
                 persistent=True,
                 logFileTMS=None):
        Daemon.__init__(self,'/tmp/TMS.%s.pid' % user)

        self.host=os.uname()[1]
        self.port=port
        self.startTime = str(datetime.now())
        self.ID = int(time())
        self.info = {}
        self.EOCString=EOCString
        self.user=user
        self.setHost( self.host )
        self.setPort( self.port )
        self.setUser( self.user )

        self.sslConnection=sslConnection
        self.keyfile=keyfile
        self.certfile=certfile
        self.ca_certs=ca_certs

        #self.tdConnection = None
        #self.tdConnection = self.openTDConnection()
        
        self.processor = processor

        # connect to database
        dbconnection = hDBConnection()

        # register user
        try:
            self.userID = dbconnection.query( db.User.id ).filter( db.User.name==self.user ).one()[0]
        except NoResultFound:
            sys.stderr.write( "Your are not allowed to use the TaskManager. Please contact your system administrator." )
            sys.exit( -1 )

        # save database ids of some entries in self.databaseIDs
        self.databaseIDs = {}
        self.initDatabaseIDs( dbconnection )

        dbconnection.remove()
        
        self.Lock = threading.Lock()

        ##cluster information
        self.cluster = {}	# {<hostID>: ServerProxy, ...}

        self.persistent=persistent		# if True, do not shutdown TMS
        self.shutdownImmediatly = False		# if True, shutdown server anyway

        self.verboseMode = verboseMode
        self.logFileTMS = logFileTMS

        # set interval for loop of calling loop functions
        self.loopInterval = 5

        # start the server
        SocketServer.TCPServer.__init__(self, (self.host,self.port), handler)

        
    def run(self):
        """! @brief Start up a server
        
        Each time a new request comes in it will be handled by a RequestHandler class
        """

        # get pid of daemon of pid of current process
        try:
            with open(self.pidfile) as f:
                self.pid = f.readline().strip()
        except:
            self.pid = os.getpid()


        try:
            # save host and port
            tmsConfig = hTaskManagerServerInfo()
            tmsConfig.save([ ('host', self.host),
                             ('port', self.port),
                             ('sslconnection', self.sslConnection),
                             ('eocstring', self.EOCString),
                             ('laststart', self.startTime),
                             ('pid', self.pid)
                             ])

            invokingTMSTime2 = datetime.now()
            invokingTMSTimeDt = invokingTMSTime2-invokingTMSTime1

            logger.info("")
            logger.info('TaskManagerServer has been started on %s:%s within %ss' % (self.host,self.port,invokingTMSTimeDt))
            logger.info("")

            self.printStatus()

            ## run server forever
            self.serve_forever()

            # wait for all active threads (except main thread) until it shuts down
            for currThread in threading.enumerate()[1:threading.activeCount()]:
                currThread.join()

        except KeyboardInterrupt:
            sys.exit(0)

        
    def serve_forever(self):
        """!@brief overwrites serve_forever of SocketServer.TCPServer"""

        ## check periodically the database in an own thread
        l2 = threading.Thread( target=self.loopCheckDatabase )
        l2.setDaemon( True )
        l2.start()
        
        while True:
            self.handle_request()

            
    def loopCheckDatabase( self ):
        """! @brief this function is executed periodically """
        
        while True:
            # instantiate new socket
            clientSock = hSocket(sslConnection=self.sslConnection,
                                 EOCString=self.EOCString,
                                 certfile=certfile,
                                 keyfile=keyfile,
                                 ca_certs=ca_certs)

            # connect to server itself
            clientSock.initSocket( self.host, self.port )

            ## send checkdatabase command
            clientSock.send( "ping" )
            clientSock.recv()
            
            # wait for self.loopInterval seconds
            sleep( self.loopInterval )
            

    def sendCommandToTaskDispatcher( self, command ):
        """! @brief send command to TaskDispatcher and receive response

        @param command (string) command known by TaskDispatcher
        """
        
        # instantiate new socket
        clientSock = hSocket(sslConnection=self.sslConnection,
                             EOCString=self.EOCString,
                             certfile=self.certfile,
                             keyfile=self.keyfile,
                             ca_certs=self.ca_certs)

        clientSock.initSocket( TaskDispatcherHost, TaskDispatcherPort )
        clientSock.send( command )
        response = clientSock.recv()

        return response
        
        
    def setHost(self,host):
        """!set host where TMS is running
        @param port host of TMS"""
        self.info['host']=host

    def setPort(self,port):
        """!set port where TMS is running
        @param port port of TMS"""
        self.info['port']=port

    def setUser(self,user):
        """!
        @brief set user of TMS

        @param user user name"""
        self.info['user']=user

    def initDatabaseIDs( self, dbcon ):
        """! @brief save some database ids in self.databaseIDs

        @param dbcon (hDBConnection) database connection
        """

        self.databaseIDs = dict( dbcon.query( db.JobStatus.name, db.JobStatus.id ).all() )

    def getClusterStatus( self ):
        """! @brief get status of cluster

        return (list) [jobCounts, slotInfo]
        """

        dbconnection = hDBConnection()
        
        # get all number of jobs for each status type for user
        counts = dict( dbconnection.query( db.JobStatus.name, func.count('*') ).\
                       join( db.JobDetails, db.JobDetails.job_status_id==db.JobStatus.id ).\
                       join( db.Job, db.Job.id==db.JobDetails.job_id ).\
                       filter( db.Job.user_id==self.userID ).\
                       group_by( db.JobStatus.name ).\
                       all() )

        if not counts:
            # no jobs so far in the database
            counts = {}
        
        slotInfo = dbconnection.query( func.count('*'),
                                       func.sum( db.Host.max_number_occupied_slots ), 
                                       func.sum( db.HostSummary.number_occupied_slots ) ).select_from( db.Host ).join( db.HostSummary, db.HostSummary.host_id==db.Host.id ).filter( db.HostSummary.active==True ).one()

        if slotInfo[0]==0:
            slotInfo = (0, 0, 0)
            
        dbconnection.remove()

        return [counts,slotInfo]
        
    def printStatus(self):
        """!@brief print status of server to stdout"""

        counts,slotInfo = self.getClusterStatus()
        
        print "----------------------------"
        logger.info( "Status of TaskManagerServer on {h}:{p} of user {u}".format(t=str(datetime.now()), h=self.host, p=self.port, u=self.user) )
        print
        print "{s:>20} : {value}".format(s="active hosts", value=slotInfo[0] )
        print "{s:>20} : {value}".format(s="occupied slots", value="{occupied} / {total}".format(occupied=slotInfo[2],total=slotInfo[1]) )
        print "{s:>20} : {value}".format(s="waiting jobs", value=counts.get('waiting',0) )
        print "{s:>20} : {value}".format(s="pending jobs", value=counts.get('pending',0) )
        print "{s:>20} : {value}".format(s="running jobs", value=counts.get('running',0) )
        print "{s:>20} : {value}".format(s="finished jobs", value=counts.get('finished',0) )
        print "----------------------------"



        
# The RequestHandler handles an incoming request.
class TaskManagerServerHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, clientAddress, TMS):
        self.request = request
        self.requestHost,self.requestPort = self.request.getpeername()
        self.TMS = TMS
        self.firstRequest = True
        self.persistentSocket = False	# socket can be set as persistent, i.e., server does not close this socket
        self.waitForNextRequest = False # do not shutdown server but wait for next request
        self.currThread = threading.currentThread()

        SocketServer.BaseRequestHandler.__init__(self, request, clientAddress, self.TMS)

    def finish(self):
        # check for termination of TMS
        # do not shutdown if
        #    - server is set as persistent
        #    - shutdownLater flag is set
        #    -
        if self.waitForNextRequest:
            logger.info("server is waiting for next request ...")
        elif self.TMS.shutdownImmediatly:
            logger.info("server shutdown ...")
            self.TMS.shutdown()
        #elif not self.TMS.persistent and self.TMS.waitingJobs==[] and self.TMS.pendingJobs==[] and self.TMS.runningJobs==[]:
        elif not self.TMS.persistent:
            logger.info("last thread (%s) has been deleted" % threading.currentThread().getName())
            logger.info("server shutdown ...")

            self.TMS.shutdown()

        sys.stdout.flush()

    def handle(self):
        # wait until lock has been released
        # lock is acquired, e.g., during addjobJSON request
        #self.TMS.Lock.acquire()
        #self.TMS.Lock.release()

        logger.info("-----------------------")
        logger.info("TMS (%s:%s) has created a new thread (%s) for connection from %s:%s" % (self.TMS.host,
                                                                                             self.TMS.port,
                                                                                             self.currThread.getName(),
                                                                                             self.requestHost,
                                                                                             self.requestPort))

        while self.firstRequest or self.persistentSocket or self.waitForNextRequest:
            self.firstRequest = False
            self.waitForNextRequest = False
            
            (sread, swrite, sexc) = select.select([self.request], [], [], None)

            logger.info("-----------------------")

            if not self.persistentSocket:
                # instantiate new socket
                hSock = hSocket(sock=self.request,
                                serverSideSSLConn=True,
                                sslConnection=self.TMS.sslConnection,
                                EOCString=self.TMS.EOCString,
                                certfile=self.TMS.certfile,
                                keyfile=self.TMS.keyfile,
                                ca_certs=self.TMS.ca_certs)

            receivedStr = hSock.recv()

            logger.info("[{name}] NEW REQUEST: {s}".format(name=self.currThread.getName(), s=receivedStr) )

            # process request
            try:
                self.persistentSocket, self.waitForNextRequest = self.TMS.processor.process(receivedStr, hSock, self.persistentSocket, self.TMS)
            except:
                # processing failed
                tb = sys.exc_info()

                sys.stdout.write('[%s] [%s:%s] Error while processing request!\n' % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),self.requestHost,self.requestPort))
                sys.stdout.flush()

                # maybe output to stderr?
                traceback.print_exception(*tb,file=sys.stdout)
                sys.stdout.flush()

                hSock.send("Error while processing request!\n%s" %  tb[1])

            logger.info("-----------------------")

        logger.info("TMS (%s:%s) has deleted %s" % (self.TMS.host,
                                                    self.TMS.port,
                                                    self.currThread.getName()))

        self.TMS.printStatus()
        
        # closing socket
        try:
            hSock.close()
        except:
            pass

        del hSock
        sys.stdout.flush()



class TaskManagerServerProcessor(object):
    def __init__(self):
        
        ############
        # define commands
        ############
        # specific help could be specified here as well
        self.commands = {}	# {<COMMAND>: hCommand, ...}

        self.commands["PING"] = hCommand(command_name = 'ping',
                                         regExp = '^ping$',
                                         help = "return pong")
        
        self.commands["CHECK"] = hCommand(command_name = 'check',
                                         regExp = '^check$',
                                         help = "return an ok")

        self.commands["HELP"] = hCommand(command_name = "help",
                                        regExp = "^help$",
                                        help = "return help")

        self.commands["ADDJOB"] = hCommand(command_name = 'addjob',
                                           arguments = "<jsonStr>",
                                           regExp = 'addjob:(.*)',
                                           help = "add job to TMS.")

        self.commands["ADDJOBS"] = hCommand(command_name = 'addjobs',
                                           arguments = "<jsonStr>",
                                           regExp = 'addjobs:(.*)',
                                           help = "add severla jobs at once to TMS.")
        
        self.commands["SETALLPJOBSASWAITING"] = hCommand(command_name = 'setallpjobsaswaiting',
                                                         regExp = '^setallpjobsaswaiting$',
                                                         help = "set all pending jobs as waiting. free occupied slots on hosts.")
        
        self.commands["SETALLPJOBSASFINISHED"] = hCommand(command_name = 'setallpjobsasfinished',
                                                         regExp = '^setallpjobsasfinished$',
                                                         help = "set all pending jobs as finished. free occupied slots on hosts.")
        
        self.commands["SETPERSISTENT"] = hCommand(command_name = 'setpersistent',
                                                  regExp = '^setpersistent$',
                                                  help = "set socket connection persistent, i.e. do not close socket")
        
        self.commands["UNSETPERSISTENT"] = hCommand(command_name = 'unsetpersistent',
                                                   regExp = '^unsetpersistent$',
                                                   help = "unset socket connection persistent, i.e. do not close socket")
        

        self.commands["SETINTERVAL"] = hCommand( command_name = "setinterval",
                                                 arguments = "<TIME>",
                                                 regExp = "^setinterval:(.*)",
                                                 help = "set interval in seconds for loop checking the database")
        
        self.commands["PRINTSERVERINFO"] = hCommand(command_name = 'printserverinfo',
                                               regExp = '^printserverinfo$',
                                               help = "return info about task manager server and taskdispatcher.")
        
        
        self.commands["GETTDSTATUS"] = hCommand(command_name = 'gettdstatus',
                                               regExp = '^gettdstatus$',
                                               help = "return status of task dispatcher")
        
        self.commands["LSTHREADS"] = hCommand(command_name = "lsthreads",
                                            regExp = "^lsthreads$",
                                            help = "return list of active threads")
        
        self.commands["LSACTIVECLUSTER"] = hCommand(command_name = 'lsactivecluster',
                                                   regExp = '^lsactivecluster$',
                                                   help = "return active cluster hosts")
        
        self.commands["LAACTIVECLUSTER"] = hCommand(command_name = 'laactivecluster',
                                                   regExp = '^laactivecluster$',
                                                   help = "return (formated) information about active cluster")
        
        self.commands["LSWJOBS"] = hCommand(command_name = 'lswjobs',
                                           regExp = '^lswjobs$',
                                           help = "return waiting jobs")
        
        self.commands["LSPJOBS"] = hCommand(command_name = 'lspjobs',
                                           regExp = '^lspjobs$',
                                           help = "return pending jobs")
        
        self.commands["LSRJOBS"] = hCommand(command_name = 'lsrjobs',
                                           regExp = '^lsrjobs$',
                                           help = "return running jobs")
        
        self.commands["LSFJOBS"] = hCommand(command_name = 'lsfjobs',
                                           regExp = 'lsfjobs',
                                           help = "return finished jobs")
        
        self.commands["LSJOB"] = hCommand(command_name = 'lsjob',
                                          arguments = "<jobID>",
                                          regExp = 'lsjob:(.*)',
                                          help = "return job info about job with given jobID")
        
        self.commands["FINDJOBS"] = hCommand(command_name = 'findjobs',
                                            arguments = "<matchString>",
                                            regExp = 'findjobs:(.*)',
                                            help = "return all jobs which match the search string in command, info text and group.")
        
        ##self.commands["ADDJOB"] = hCommand(command_name = 'addjob',
        ##                                  arguments = "<infoText>:<command>:<logFile>:<shell>:<priority>",
        ##                                  regExp = 'addjob:([^:]*):([^:]*):([^:]*):([^:]*):([^:]*)',
        ##                                  help = "add job to TMS")
        
        self.commands["ADDJOBJSON"] = hCommand(command_name = 'addjobJSON',
                                              arguments = "<jsonStr>",
                                              regExp = 'addjobJSON:(.*)',
                                              help = "add job to TMS as json string")
        
        self.commands["RUNJOB"] = hCommand(command_name = 'runjob',
                                            arguments = "<jsonStr>",
                                            regExp = 'runjob:(.*)',
                                            help = "authorization from task disaptcher to sent job to a certain host")
        
        self.commands["SUSPENDJOB"] = hCommand(command_name = 'suspendjob',
                                              arguments = "<jobID>",
                                              regExp = 'suspendjob:(.*)',
                                              help = "suspend job with jobID")
        
        self.commands["RESUMEJOB"] = hCommand(command_name = 'resumejob',
                                             arguments = "<jobID>",
                                             regExp = 'resumejob:(.*)',
                                             help = "resume job with jobID")
        
        self.commands["KILLJOB"] = hCommand(command_name = 'killjob',
                                           arguments = "<jobID>",
                                           regExp = '^killjob:(.*)$',
                                           help = "kill job with jobID")
        
        self.commands["KILLJOBS"] = hCommand(command_name = 'killjobs',
                                            arguments = "<jobID>[:<jobID>:...]",
                                            regExp = '^killjobs:(.*)$',
                                            help = "kill jobs with jobID")
        
        self.commands["KILLMATCHINGJOBS"] = hCommand(command_name = 'killmatchingjobs',
                                                    arguments = "<matchString>",
                                                    regExp = 'killmatchingjobs:(.*)',
                                                    help = "kill all jobs which match the given string as regular expression")
        
        self.commands["KILLALLJOBS"] = hCommand(command_name = 'killalljobs',
                                               regExp = '^killalljobs$',
                                               help = "kill all jobs")
        
        self.commands["PROCESSSTARTED"] = hCommand(command_name = 'ProcessStarted',
                                                  arguments = "<jsonString>",
                                                  regExp = '^ProcessStarted:(.*)',
                                                  help = "Info that job has been started.")
        
        self.commands["PROCESSFINISHED"] = hCommand(command_name = 'ProcessFinished',
                                                   arguments = "<jobID>",
                                                   regExp = '^ProcessFinished:(.*)',
                                                   help = "Info that job has been finished")
        
        self.commands["TMMSSTARTED"] = hCommand(command_name = 'TMMSStarted',
                                               arguments = "<jsonString>",
                                               regExp = 'TMMSStarted:(.*)',
                                               help = "Info that a TMMS has been started")
        
        self.commands["LSHISTORY"] = hCommand(command_name = 'lshistory',
                                               arguments = "<epoch>",
                                               regExp = 'lshistory:(.*)',
                                               help = "get all events since epoch")
        
        self.commands["SHUTDOWN"] = hCommand(command_name = 'shutdown',
                                             regExp = '^shutdown$',
                                             help = "shutdown TMS")


    def process(self, receivedStr, request, persistentSocket, TMS):
        """! @brief process request string
        @param s received string
        @param request request of client
        @param persistentSocket True|False as indicator for persistent socket
        @param TMS instance of TaskManagerServer
        @return persistentSocket flag if socket is persistent
        @return waitForNextRequest flag for server to wait for next request
        """
        # log processing time
        procTime1 = datetime.now()

        self.TMS = TMS

        h,p = request.socket.getpeername()
        currThread = threading.currentThread()
        threadName = currThread.getName()

        waitForNextRequest = False

        if not receivedStr:
            logger.info('[%s] ... socket has been closed' % threadName)
            return False, False

        #####################
        # grep TDHost and TDPort
        #reTD = re.compile('^TD:([^:]*):([^:]*)(.*)$')
        #if reTD.match(receivedStr):
        #    TDHost = reTD.match(receivedStr).groups()[0]
        #    TDPort = int(reTD.match(receivedStr).groups()[1])
        #    receivedStr = reTD.match(receivedStr).groups()[2]
        #else:
        #    # use global definition
        TDHost = copy(TaskDispatcherHost)
        TDPort = copy(TaskDispatcherPort)


        #####################
        # check if request is a known command

        if self.commands["PING"].re.match(receivedStr):
            request.send("pong")
            waitForNextRequest = True

        ########
        # check
        #    response an ok
        #    do not shut down task manager server after this command
        elif self.commands["CHECK"].re.match(receivedStr):
            request.send("ok")
            waitForNextRequest = True
            
        #  get help
        elif self.commands["HELP"].re.match(receivedStr):
            h = []
            h.append("Commands:")

            # loop over all command which should be shown in each category

            # general commands
            h.append("  general commands:")
            l = [
                "CHECK",
                "HELP",
                "LSTHREADS",
                "SETPERSISTENT",
                "UNSETPERSISTENT",
                "GETTDSTATUS",
                "LSACTIVECLUSTER",
                "LAACTIVECLUSTER",
                "SHUTDOWN"
                ]
            h.extend(renderHelp(l,self.commands))
            h.append("")

            h.append("  job commands:")

            # job commands
            l = [
                "LSWJOBS",
                "LSPJOBS",
                "LSRJOBS",
                "LSFJOBS",
                "FINDJOBS",
                "ADDJOB",
                "SUSPENDJOB",
                "RESUMEJOB",
                "KILLJOB",
                "KILLJOBS",
                "KILLMATCHINGJOBS",
                "KILLALLJOBS",
                "PROCESSSTARTED",
                "PROCESSFINISHED"
                ]
            h.extend(renderHelp(l,self.commands))

            request.send(join(h,'\n'))


        #  get list of active threads
        elif self.commands["LSTHREADS"].re.match(receivedStr):
            request.send(join(map(lambda t: t.getName(),threading.enumerate()),"\n"))

        #  set socket connection persistent, i.e. do not close socket
        elif self.commands["SETPERSISTENT"].re.match(receivedStr):
            request.send("connection has been set persistent")
            persistentSocket = True

        #  unset socket connection persistent
        elif self.commands["UNSETPERSISTENT"].re.match(receivedStr):
            request.send("connection has been set unpersistent")
            persistentSocket = False

        # set interval for loop
        elif self.commands['SETINTERVAL'].re.match(receivedStr):
            c = self.commands["SETINTERVAL"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopInterval = interval

            request.send('done.')
            
        #  get status of task dispatcher
        elif self.commands["GETTDSTATUS"].re.match(receivedStr):
            com = "gettdstatus"

            try:
                response = self.TMS.sendCommandToTaskDispatcher( com )
                request.send( response )
            except:
                traceback.print_exc(file=sys.stderr)
                request.send("Could not connect to TaskDispatcher.")

        #  get info about task manager server and taskdispatcher
        elif self.commands["PRINTSERVERINFO"].re.match(receivedStr):

            tdstatus = self.TMS.sendCommandToTaskDispatcher( "gettdstatus" )
            tdstatus = json.loads(tdstatus)
            
            jobCounts, slotInfo = self.TMS.getClusterStatus()

            response = ""
            response += "Status of TaskManagerServer on {h}:{p} of user {u}\n".format(t=str(datetime.now()), h=self.TMS.host, p=self.TMS.port, u=self.TMS.user)
            response += "\n"
            response += "{s:>35} : {value}\n".format(s="tms running since", value=self.TMS.startTime )
            response += "{s:>35} : {value}\n".format(s="taskdispatcher host", value=TDHost )
            response += "{s:>35} : {value}\n".format(s="taskdispatcher port", value=TDPort )
            response += "{s:>35} : {value}\n".format(s="taskdispatcher activity status", value=tdstatus['activity status'] )
            response += "{s:>35} : {value}\n".format(s="active hosts", value=slotInfo[0] )
            response += "{s:>35} : {value}\n".format(s="occupied slots", value="{occupied} / {total}".format(occupied=slotInfo[2],total=slotInfo[1]) )
            response += "\n"
            response += "{s:>35} : {value}\n".format(s="waiting jobs", value=jobCounts.get('waiting',0) )
            response += "{s:>35} : {value}\n".format(s="pending jobs", value=jobCounts.get('pending',0) )
            response += "{s:>35} : {value}\n".format(s="running jobs", value=jobCounts.get('running',0) )
            response += "{s:>35} : {value}\n".format(s="finished jobs", value=jobCounts.get('finished',0) )

            request.send( response )

        #  get active hosts in cluster
        elif self.commands["LSACTIVECLUSTER"].re.match(receivedStr):
            # send request to task dispatcher
            com = "lsactivecluster"
            tdConn = TMConnection(TDHost,
                                  TDPort,
                                  sslConnection=self.TMS.sslConnection,
                                  keyfile=keyfile,
                                  certfile=certfile,
                                  ca_certs=ca_certs,
                                  catchErrors=False,
                                  loggerObj=logger)

            if tdConn.openConnection:
                tdConn.sendAndRecvAndClose(com)
                if tdConn.requestSent:
                    request.send(tdConn.response)
                else:
                    request.send("no info")
            else:
                request.send("Connection to TD failed")

        #  show (formated) information about active cluster
        elif self.commands["LSACTIVECLUSTER"].re.match(receivedStr):
            # send request to task dispatcher
            com = "laactivecluster"
            tdConn = TMConnection(TDHost,
                                  TDPort,
                                  sslConnection=self.TMS.sslConnection,
                                  keyfile=keyfile,
                                  certfile=certfile,
                                  ca_certs=ca_certs,
                                  catchErrors=False,
                                  loggerObj=logger)

            if tdConn.openConnection:
                tdConn.sendAndRecvAndClose(com)
                if tdConn.requestSent:
                    request.send(tdConn.response)
                else:
                    request.send("no info")
            else:
                request.send("Connection to TD failed")

        #  get waiting jobs
        elif self.commands["LSWJOBS"].re.match(receivedStr):
            # connect to database
            dbconnection = hDBConnection()
            
            wJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                    db.JobDetails.job_status_id==self.TMS.databaseIDs['waiting'] ) ).all()

            response = ""
            for idx,job in enumerate(wJobs):
                response += "{i} - [jobid:{id}] [status:waiting since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                              id=job.id,
                                                                                                                                              t=str(job.job_history[-1].datetime),
                                                                                                                                              group=job.group,
                                                                                                                                              info=job.info_text,
                                                                                                                                              command=job.command[:30],
                                                                                                                                              dots="..." if len(job.command)>30 else "" )

            if response:
                request.send( response )
            else:
                request.send("no waiting jobs")

            dbconnection.remove()

        #  get pending jobs
        elif self.commands["LSPJOBS"].re.match(receivedStr):
            # connect to database
            dbconnection = hDBConnection()
            
            pJobs = dbconnection.query( db.Job,db.JobDetails ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                                 db.JobDetails.job_status_id==self.TMS.databaseIDs['pending'] ) ).all()

            response = ""
            for idx,(job,jobDetails) in enumerate(pJobs):
                response += "{i} - [jobid:{id}] [status:pending on {host} since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                        id=job.id,
                                                                                                                                                        host=jobDetails.host.short_name,
                                                                                                                                                        t=str(job.job_history[-1].datetime),
                                                                                                                                                        group=job.group,
                                                                                                                                                        info=job.info_text,
                                                                                                                                                        command=job.command[:30],
                                                                                                                                                        dots="..." if len(job.command)>30 else "" )
            if response:
                request.send( response )
            else:
                request.send("no pending jobs")

            dbconnection.remove()


        #  get running jobs
        elif self.commands["LSRJOBS"].re.match(receivedStr):
            # connect to database
            dbconnection = hDBConnection()
            
            pJobs = dbconnection.query( db.Job, db.JobDetails ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                                 db.JobDetails.job_status_id==self.TMS.databaseIDs['running'] ) ).all()

            response = ""
            for idx,(job,jobDetails) in enumerate(pJobs):
                response += "{i} - [jobid:{id}] [status:running on {host} since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                        id=job.id,
                                                                                                                                                        host=jobDetails.host.short_name,
                                                                                                                                                        t=str(job.job_history[-1].datetime),
                                                                                                                                                        group=job.group,
                                                                                                                                                        info=job.info_text,
                                                                                                                                                        command=job.command[:30],
                                                                                                                                                        dots="..." if len(job.command)>30 else "" )
            if response:
                request.send( response )
            else:
                request.send("no running jobs")

            dbconnection.remove()

        #  get finished jobs
        elif self.commands["LSFJOBS"].re.match(receivedStr):
            # connect to database
            dbconnection = hDBConnection()

            rJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                    db.JobDetails.job_status_id==self.TMS.databaseIDs['finished'] ) ).all()

            response = ""
            for idx,job in enumerate(rJobs):
                response += "{i} - [jobid:{id}] [status:finished since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                               id=job.id,
                                                                                                                                               t=str(job.job_history[-1].datetime),
                                                                                                                                               group=job.group,
                                                                                                                                               info=job.info_text,
                                                                                                                                               command=job.command[:30],
                                                                                                                                               dots="..." if len(job.command)>30 else "" )
            if response:
                request.send( response )
            else:
                request.send("no finished jobs")

            dbconnection.remove()
                
        #  get job info of job with given jobID
        elif self.commands["LSJOB"].re.match(receivedStr):
            c = self.commands["LSJOB"]

            jobID = c.re.match(receivedStr).groups()[0]

            # connect to database
            dbconnection = hDBConnection()
            
            job = dbconnection.query( db.Job ).get( int(jobID) )

            if job:
                response = ""
                response += "{s:>20} : {value}\n".format(s="job id", value=job.id )
                response += "{s:>20} : {value}\n".format(s="command", value=job.command )
                response += "{s:>20} : {value}\n".format(s="info text", value=job.info_text )
                response += "{s:>20} : {value}\n".format(s="group", value=job.group )
                response += "{s:>20} : {value}\n".format(s="stdout", value=job.stdout )
                response += "{s:>20} : {value}\n".format(s="stderr", value=job.stderr )
                response += "{s:>20} : {value}\n".format(s="logfile", value=job.logfile )
                response += "{s:>20} : {value}\n".format(s="excludedHosts", value=job.excluded_hosts )
                response += "{s:>20} : {value}\n".format(s="slots", value=job.slots )
                
                for idx,hist in enumerate(job.job_history):
                    if idx==0: s = "status"
                    else: s=""
                    
                    response += "{s:>20} : [{t}] {status}\n".format(s=s, t=str(hist.datetime), status=hist.job_status.name )
                    
                response += "{s:>20} : {value}\n".format(s="host", value=job.job_details.host.short_name )
                response += "{s:>20} : {value}\n".format(s="pid", value=job.job_details.pid )
                response += "{s:>20} : {value}\n".format(s="return code", value=job.job_details.return_code )

                request.send( response )
            else:
                request.send("unkown job.")



        #self.status = "initiated"
        #self.statusCode = 0
        #self.jobInfo = {
        #    'command': None,
        #    'jobInfo': None,
        #    'host': None,
        #    'shell': None,
        #    'logFile': None,
        #    'TaskDispatcherHost': None,
        #    'TaskDispatcherPort': None,
        #    'fileCommand': None,
        #    'fileOutput': None,
        #    'fileError': None,
        #    'pid': None,
        #    'returnCode': None
        #    }
        #self.events = []	# list of events: [(time,whatHappened)]

            ##com = 'lsjobinfo:%s' % jobID
            ##tdConn = TMConnection(TDHost,
            ##                      TDPort,
            ##                      sslConnection=self.TMS.sslConnection,
            ##                      keyfile=keyfile,
            ##                      certfile=certfile,
            ##                      ca_certs=ca_certs,
            ##                      catchErrors=False,
            ##                      loggerObj=logger)
            ##
            ##if tdConn.openConnection:
            ##    tdConn.sendAndRecvAndClose(com)
            ##    if tdConn.requestSent:
            ##        if tdConn.response:
            ##            request.send(tdConn.response)
            ##        else:
            ##            request.send("Unknown job!")
            ##    else:
            ##        request.send("Sending to TD failed")
            ##else:
            ##    request.send("Connection to TD failed")



        #  get all jobs with match the search string
        elif self.commands["LSMATCHINGJOBS"].re.match(receivedStr):
            c = self.commands["LSMATCHINGJOBS"]

            matchString = c.re.match(receivedStr).groups()[0]

            m = re.compile(matchString)

            jobList = []
            for jobID,jobInfo in self.TMS.jobsDict.iteritems():
                if m.search(jobInfo.getJobInfo('jobInfo')):
                    jobList.append(jobID)

            request.send( self.formatJobList(jobList) )

        #  add job to TMS and then to task dispatcher
        ##elif TMS.commands["ADDJOB"].re.match(receivedStr):
        ##    c = TMS.commands["ADDJOB"]
        ##    jobInfo,command,logFile,shell,priority = c.re.match(receivedStr).groups()
        ##
        ##    # jobID will now be generated by TD
        ##    jobID = TMS.info['user']
        ##
        ##    logger.info('[%s] ... sending job to TD' % threadName)
        ##
        ##    # send command to TaskDispatcher
        ##    if (not priority):
        ##        com = "addjob:%s:%s:%s:%s:%s:%s" % (TMS.info['host'],
        ##                                             TMS.info['port'],
        ##                                             TMS.ID,
        ##                                             jobInfo,
        ##                                             command,
        ##                                             jobID)
        ##    else:
        ##        com = "addjob:%s:%s:%s:%s:%s:%s:%s" %(TMS.info['host'],
        ##                                               TMS.info['port'],
        ##                                               TMS.ID,
        ##                                               jobInfo,
        ##                                               command,
        ##                                               jobID,
        ##                                               priority)
        ##
        ##    # send command to task dispatcher
        ##    tdConn = TMConnection(TDHost,
        ##                          TDPort,
        ##                          sslConnection=TMS.sslConnection,
        ##                          keyFile=keyFile,
        ##                          certfile=certfile,
        ##                          ca_certs=ca_certs,
        ##                          catchErrors=False,
        ##                          loggerObj=logger)
        ##
        ##    if tdConn.openConnection:
        ##        tdConn.sendAndRecvAndClose(com)
        ##        if tdConn.requestSent:
        ##            # successful response
        ##            jobID = tdConn.response
        ##
        ##            # add job as new instance
        ##            TMS.jobsDict[jobID] = Job()
        ##
        ##            TMS.jobsDict[jobID].setJobInfo(
        ##                command = command,
        ##                jobInfo = jobInfo,
        ##                logFile = logFile,
        ##                shell = shell,
        ##                TaskDispatcherHost = TDHost,
        ##                TaskDispatcherPort = TDPort
        ##                )
        ##            TMS.jobsDict[jobID].setAsAdded()
        ##
        ##            TMS.waitingJobs.append(jobID)
        ##
        ##            logger.info("[%s] ... ... assigned jobID: %s" % (threadName,jobID))
        ##
        ##            request.send("Job [%s] has been submitted to TaskDispatcher %s:%s.\nSo long, and thanks for all the fish." % (jobID,TDHost,TDPort))
        ##
        ##            self.executeJobEvent(request,h,p,threadName,jobID,TMS)
        ##        else:
        ##            request.send("TMS error while sending to TD %s:%s" % (TDHost,TDPort))
        ##
        ##            logger.info("[%s] ... sent failed" % threadName)
        ##    else:
        ##        request.send("TMS error while connecting to TD %s:%s" % (TDHost,TDPort))
        ##
        ##        logger.info("[%s] ... sent failed" % threadName)

        #  add job and register job to task dispatcher
        elif self.commands["ADDJOB"].re.match(receivedStr):
            c = self.commands["ADDJOB"]

            jsonInObj = c.re.match(receivedStr).groups()[0]
            jsonInObj = json.loads(jsonInObj)

            command = jsonInObj['command']
            infoText = jsonInObj['infoText']
            group = jsonInObj['group']
            stdout = jsonInObj['stdout']
            stderr = jsonInObj['stderr']
            logfile = jsonInObj['logfile']
            shell = jsonInObj['shell']
            priority = jsonInObj['priority']
            excludedHosts = jsonInObj.get("excludedHosts","")
            user = self.TMS.info['user']

            logger.info('[%s] ... send job to TD' % threadName)

            # register job at TaskDispatcher
            jsonOutObj =  { 'TMSHost': self.TMS.info['host'],
                            'TMSPort': self.TMS.info['port'],
                            'TMSID': self.TMS.ID,
                            'infoText': infoText,
                            'group': group,
                            'command': command,
                            'shell': shell,
                            'stdout': stdout,
                            'stderr': stderr,
                            'logfile': logfile,
                            'user': user,
                            'priority': priority,
                            'excludedHosts': excludedHosts}

            jsonOutObj = json.dumps(jsonOutObj)
            com = "addjob:%s" % jsonOutObj

            try:
                jobID = self.TMS.sendCommandToTaskDispatcher( com )

                response = "Job [{id}] has been submitted to TaskDispatcher.\nSo long, and thanks for all the fish.".format(id=jobID)
                request.send( response )
            except:
                traceback.print_exc(file=sys.stderr)
                request.send("Could not connect to TaskDispatcher.")


        #  add several jobs and register these to task dispatcher
        elif self.commands["ADDJOBS"].re.match(receivedStr):
            c = self.commands["ADDJOBS"]

            jsonInObj = c.re.match(receivedStr).groups()[0]
            jsonInObj = json.loads(jsonInObj)

            user = self.TMS.info['user']
            jobs = jsonInObj	# list of dictinaries

            # register job at TaskDispatcher
            jsonOutObj =  { 'user': user,
                            'TMSHost': self.TMS.info['host'],
                            'TMSPort': self.TMS.info['port'],
                            'TMSID': self.TMS.ID,
                            'jobs': jobs }

            logger.info('[%s] ... send jobs to TD' % threadName)

            jsonOutObj = json.dumps(jsonOutObj)
            com = "addjobs:%s" % jsonOutObj

            try:
                jobIDs = self.TMS.sendCommandToTaskDispatcher( com )
                jobIDs = json.loads( jobIDs )
                
                response = "Jobs {ids} have been submitted to TaskDispatcher.\nSo long, and thanks for all the fish.".format(ids=jobIDs)
                request.send( response )
            except:
                traceback.print_exc(file=sys.stderr)
                request.send("Could not connect to TaskDispatcher.")

                
        #  authorization from task dispatcher to run a job on given host
        elif self.commands["RUNJOB"].re.match( receivedStr ):
            c = self.commands["RUNJOB"]

            jobInfo = json.loads( c.re.match( receivedStr ).groups()[0] )

            jobID = jobInfo['jobID']
            hostID = jobInfo['hostID']
            hostName = jobInfo['hostName']

            # invoke TMMS if necessary and add to cluster
            if not hostID in self.TMS.cluster:
                logger.info('[{th}] Invoke a TMMS'.format(th=threadName))
                TMMS = hServerProxy( user = self.TMS.user,
                                     host = hostName,
                                     serverType = 'TMMS',
                                     sslConnection = self.TMS.sslConnection,
                                     keyfile = self.TMS.keyfile,
                                     certfile = self.TMS.certfile,
                                     ca_certs = self.TMS.ca_certs )
                TMMS.run()
                
                if not TMMS.running:
                    sys.stderr.write("Could not start a TMMS!\n")
                    sys.exit(-1)
                else:
                    logger.info('[{th}] ... TMMS has been started on {h}:{p}'.format(th=threadName, h=TMMS.host, p=TMMS.port) )

                self.TMS.cluster[hostID] = TMMS
            else:
                TMMS = self.TMS.cluster[hostID]

            # send job to TMMS
            jsonObj = { 'jobID': jobID }
            jsonObj = json.dumps( jsonObj )
            
            command = 'runjob:{j}'.format( j=jsonObj )

            logger.info('[{th}] ... send job to TMMS on {h}:{p}'.format(th=threadName, h=TMMS.host, p=TMMS.port) )
            ret = TMMS.send( command, createNewSocket=True )

            if ret!=True and ret.strerror=="Connection refused":
                # remove TMMS
                del self.TMS.cluster[ hostID ]

                logger.info('[{th}] ... failed. send job again to TMS.'.format(th=threadName, h=TMMS.host, p=TMMS.port) )
                
                clientSock = hSocket(sslConnection=self.TMS.sslConnection,
                                     EOCString=self.TMS.EOCString,
                                     certfile=self.TMS.certfile,
                                     keyfile=self.TMS.keyfile,
                                     ca_certs=self.TMS.ca_certs)

                clientSock.initSocket( self.TMS.host, self.TMS.port )
                clientSock.send( receivedStr )
                

            ##jsonInObj = c.re.match(receivedStr ).groups()[0]
            ##jsonInObj = json.loads(jsonInObj)
            ##
            ##jobID = jsonInObj["jobID"]
            ##hostName = jsonInObj["host"]
            ##
            ##jobStatus = self.TMS.checkJobID(jobID)
            ##if jobStatus == 2:
            ##    # job is known by this TMS - start job
            ##
            ##    # create cluster host instance if necessary
            ##    if not hostName in self.TMS.cluster:
            ##        self.TMS.cluster[hostName] = ClusterHost(hostName,
            ##                                                 TDHost,
            ##                                                 TDPort,
            ##                                                 self.TMS.host,
            ##                                                 self.TMS.port)
            ##
            ##    # set job properties
            ##    self.TMS.jobsDict[jobID].setJobInfo( host = hostName )
            ##
            ##    command = self.TMS.jobsDict[jobID].getJobInfo('command')
            ##    shell = self.TMS.jobsDict[jobID].getJobInfo('shell')
            ##    logFile = self.TMS.jobsDict[jobID].getJobInfo('logFile')
            ##
            ##    # send job to TMMS
            ##    rt = self.runJob(jobID)
            ##
            ##    if rt:
            ##        # starting of job has failed
            ##
            ##        # send info to task dispatcher
            ##        com = 'ProcessStartFailed:%s:%s' % (jobID,rt)
            ##
            ##        tdConn = TMConnection(TDHost,
            ##                              TDPort,
            ##                              sslConnection=self.TMS.sslConnection,
            ##                              keyfile=keyfile,
            ##                              certfile=certfile,
            ##                              ca_certs=ca_certs,
            ##                              catchErrors=False,
            ##                              loggerObj=logger)
            ##
            ##        tdConn.sendAndClose(com)
            ##
            ##        logger.info("[%s] ... failed [%s] with return code [%s]" % (threadName,jobID,rt))
            ##
            ##        # set job status to 1 (same as added)
            ##        self.TMS.setJobAs(jobID,"failed")
            ##
            ##    else:
            ##        tmmsPort = self.TMS.cluster[hostName].tmmsPort
            ##        logger.info("[%s] ... job %s has been successfully sent to TMMS %s:%s" % (threadName,jobID,hostName,tmmsPort))
            ##
            ##
            ##
            ##    ##self.TMS.jobsDict[jobID].setJobAsPening()
            ##    ##self.setAs("pending",jobID)
            ##    ##
            ##    ###self.TMS.jobsDict[jobID].setAsPending()
            ##    ##self.executeJobEvent(request,threadName,jobID)
            ##
            ##
            ##elif jobStatus == 1:
            ##    ### job comes from this TMS but is unknwon. this could be due to a delayed response of TD while processing ADDJOB command
            ##    ### it is unknown which preJobID the job have/had
            ##    ##self.TMS.addUnknownJob(jobID)
            ##    ##self.TMS.unknownJob[jobID].setJobInfo(host = hostName)
            ##    ##
            ##    ### create cluster host instance if necessary
            ##    ##if not hostName in self.TMS.cluster:
            ##    ##    self.TMS.cluster[hostName] = ClusterHost(hostName)
            ##    ##    self.TMS.cluster[hostName].setTMMSPort(tmmsPort)
            ##    pass
            ##
            ##elif jobStatus == 0:
            ##    # job does not come from this TMS
            ##
            ##    logger.info("[%s] ... ... unknown job: %s" % (threadName,jobID))
            ##
            ##    # send request to td to remove this job ??????
            ##    com = "killjob:%s" % jobID
            ##    tdConn = TMConnection(TDHost,
            ##                          TDPort,
            ##                          sslConnection=self.TMS.sslConnection,
            ##                          keyfile=keyfile,
            ##                          certfile=certfile,
            ##                          ca_certs=ca_certs,
            ##                          catchErrors=True,
            ##                          loggerObj=logger)
            ##
            ##    tdConn.sendAndClose(com)
            ##
            ##    return persistentSocket,waitForNextRequest

        #####  suspend a certain job
        ####elif self.commands["SUSPENDJOB"].re.match(receivedStr):
        ####    c = self.commands["SUSPENDJOB"]
        ####
        ####    computer,PID = c.re.match(receivedStr).groups()
        ####    self.suspendJob(computer,PID)
        ####
        ####    # change status of job
        ####
        #####  resume a certain job
        ####elif self.commands["RESUMEJOB"].re.match(receivedStr):
        ####    c = self.commands["RESUMEJOB"]
        ####
        ####    computer,PID = re.match('resumejob:(.*):(.*)', receivedStr).groups()
        ####    self.resumeJob(computer,PID)
        ####
        ####    # change status of job
        ####
        ####
        #####  send killing request to TMMS
        ####elif self.commands["KILLJOB"].re.match(receivedStr):
        ####    c = self.commands["KILLJOB"]
        ####    jobID = c.re.match(receivedStr).groups()[0]
        ####
        ####    if jobID not in self.TMS.jobsDict:
        ####        logger.info("unkown job to kill: %s" % (jobID))
        ####        ret = "Job unknown!"
        ####    else:
        ####        # get host and port of TMMS
        ####        tmmsHost = self.TMS.jobsDict[jobID].getJobInfo('host')
        ####        tmmsPort = self.TMS.cluster[tmmsHost].tmmsPort
        ####
        ####        t = killJobs([jobID],tmmsHost,tmmsPort,self.TMS)
        ####        t.start()
        ####        t.join()
        ####
        ####        ret = t.ret
        ####
        ####    request.send(ret)
        ####
        ####
        #####    send killing request to TMMS
        ####elif self.commands["KILLJOBS"].re.match(receivedStr):
        ####    c = self.commands["KILLJOBS"]
        ####
        ####    jobIDs = c.re.match(receivedStr).groups()[0]
        ####    jobIDs = jobIDs.split(":")
        ####
        ####    hostList = {}
        ####    for jobID in jobIDs:
        ####        if jobID in self.TMS.jobsDict:
        ####            # get host and port of TMMS
        ####            tmmsHost = self.TMS.jobsDict[jobID].getJobInfo('host')
        ####            tmmsPort = self.TMS.cluster[tmmsHost].tmmsPort
        ####
        ####            if tmmsHost in hostList:
        ####                hostList[tmmsHost]['jobIDs'].append(jobID)
        ####            else:
        ####                hostList[tmmsHost] = {'tmmsPort':tmmsPort, 'jobIDs':[jobID]}
        ####
        ####    threadList = []
        ####    for tmmshHost,l in hostList.iteritems():
        ####        tmmsPort = l['tmmsPort']
        ####        jobIDs = l['jobIDs']
        ####
        ####        current = killJobs(jobIDs,tmmsHost,tmmsPort,self.TMS)
        ####        threadList.append(current)
        ####        current.start()
        ####
        ####    # wait until all jobs on every tmms has been killed
        ####    for t in threadList:
        ####        t.join()
        ####
        ####    request.send("Jobs has been killed.")
        ####
        ####
        #####    kill all jobs with match the given string
        ####elif self.commands["KILLMATCHINGJOBS"].re.match(receivedStr):
        ####    c = self.commands["KILLMATCHINGJOBS"]
        ####    matchString = c.re.match(receivedStr).groups()[0]
        ####
        ####    m = re.compile(matchString)
        ####
        ####    hostList = {}
        ####    for jobID,jobInfo in self.TMS.jobsDict.iteritems():
        ####        if m.search(jobInfo.getJobInfo('jobInfo')):
        ####            # get host and port of TMMS
        ####            tmmsHost = self.TMS.jobsDict[jobID].getJobInfo('host')
        ####            tmmsPort = self.TMS.cluster[tmmsHost].tmmsPort
        ####
        ####            if tmmsHost in hostList:
        ####                hostList[tmmsHost]['jobIDs'].append(jobID)
        ####            else:
        ####                hostList[tmmsHost] = {'tmmsPort':tmmsPort, 'jobIDs':[jobID]}
        ####
        ####    threadList = []
        ####    for tmmshHost,l in hostList.iteritems():
        ####        tmmsPort = l['tmmsPort']
        ####        jobIDs = l['jobIDs']
        ####
        ####        current = killJobs(jobIDs,tmmsHost,tmmsPort,self.TMS)
        ####        threadList.append(current)
        ####        current.start()
        ####
        ####    # wait until all jobs on every tmms has been killed
        ####    for t in threadList:
        ####        t.join()
        ####
        ####    request.send("Jobs has been killed.")
        ####
        ####
        #####  send killing request to TMMS
        ####elif self.commands["KILLALLJOBS"].re.match(receivedStr):
        ####    hostList = []
        ####    for hostName in self.TMS.cluster:
        ####        tmmsPort = self.TMS.cluster[hostName].tmmsPort
        ####
        ####        current = killAllJobs(hostName,tmmsPort,self.TMS)
        ####        hostList.append(current)
        ####        current.start()
        ####
        ####    # wait until all jobs on every tmms has been killed
        ####    for h in hostList:
        ####        h.join()
        ####
        ####
        ####    request.send("done")


        ###  info from TMMS about a started job
        ##elif self.commands["PROCESSSTARTED"].re.match(receivedStr):
        ##    c = self.commands["PROCESSSTARTED"]
        ##    jsonInObj = c.re.match(receivedStr).groups()[0]
        ##    jsonInObj = json.loads(jsonInObj)
        ##
        ##    jobID = jsonInObj["jobID"]
        ##    pid = jsonInObj["pid"]
        ##    fileCommand = jsonInObj['fileCommand']
        ##    fileOutput = jsonInObj['fileOutput']
        ##    fileError = jsonInObj['fileError']
        ##
        ##    jobStatus = self.TMS.checkJobID(jobID)
        ##    if jobStatus == 0:
        ##        # job does not come from this TMS
        ##        logger.info("[%s] ... ... unknown job: %s" % (threadName,jobID))
        ##
        ##        # send request to td to remove this job ????
        ##        com = "killjob:%s" % jobID
        ##
        ##        tdConn = TMConnection(TDHost,
        ##                              TDPort,
        ##                              sslConnection=self.TMS.sslConnection,
        ##                              keyfile=keyfile,
        ##                              certfile=certfile,
        ##                              ca_certs=ca_certs,
        ##                              catchErrors=True,
        ##                              loggerObj=logger)
        ##
        ##        tdConn.sendAndClose(com)
        ##
        ##        return persistentSocket,waitForNextRequest
        ##
        ##    job = self.TMS.jobsDict[jobID]
        ##    job.setJobInfo(
        ##        pid = pid,
        ##        fileCommand = fileCommand,
        ##        fileOutput = fileOutput,
        ##        fileError = fileError
        ##        )
        ##
        ##    self.TMS.setJobAs(jobID,"running")
        ##
        ##    # --> send info to taskdispatcher
        ##    jsonObj = {'jobID': jobID,
        ##               'pid': pid,
        ##               'fileCommand': fileCommand,
        ##               'fileOutput': fileOutput,
        ##               'fileError': fileError}
        ##
        ##    jsonObj = json.dumps(jsonObj)
        ##
        ##    com = "ProcessStarted:%s" % (jsonObj)
        ##
        ##    tdConn = TMConnection(TDHost,
        ##                          TDPort,
        ##                          sslConnection=self.TMS.sslConnection,
        ##                          keyfile=keyfile,
        ##                          certfile=certfile,
        ##                          ca_certs=ca_certs,
        ##                          catchErrors=False,
        ##                          loggerObj=logger)
        ##
        ##    tdConn.sendAndClose(com)
        ##
        ##    hostName = job.getJobInfo("host")
        ##    tmmsPort = self.TMS.cluster[hostName].tmmsPort
        ##    logger.info("[%s] ... job %s has been started by TMMS %s:%s" % (threadName,jobID,hostName,tmmsPort))
        ##
        ##
        ##
        ##    #self.TMS.jobsDict[jobID].setAsStarted()
        ##    #self.executeJobEvent(request,threadName,jobID)


        #    info from TMMS about a finished job
        elif self.commands["PROCESSFINISHED"].re.match(receivedStr):
            c = self.commands["PROCESSFINISHED"]
            jobID = c.re.match(receivedStr).groups()[0]

            command = "ProcessFinished:{j}".format(j=jobID)
            self.TMS.sendCommandToTaskDispatcher( command )
            
            ##jobStatus = self.TMS.checkJobID(jobID)
            ##if jobStatus == 0:
            ##    # job does not come from this TMS
            ##    logger.info("[%s] ... ... unknown job: %s" % (threadName,jobID))
            ##
            ##    # send request to td to remove this job
            ##    com = "killjob:%s" % jobID
            ##
            ##    tdConn = TMConnection(TDHost,
            ##                          TDPort,
            ##                          sslConnection=self.TMS.sslConnection,
            ##                          keyfile=keyfile,
            ##                          certfile=certfile,
            ##                          ca_certs=ca_certs,
            ##                          catchErrors=True,
            ##                          loggerObj=logger)
            ##
            ##    tdConn.sendAndClose(com)
            ##
            ##    return persistentSocket,waitForNextRequest

            ##job = self.TMS.jobsDict[jobID]

            ##job.setJobInfo(returnCode=returncode)
            ##self.TMS.setJobAs(jobID,"finished")
            ###self.TMS.jobsDict[jobID].setAsFinished()
            ###self.executeJobEvent(request,threadName,jobID)
            ##
            ### --> send info to taskdispatcher
            ##returnCode = job.getJobInfo('returnCode')

            #com = "ProcessFinished:%s" % (jobID)
            #
            #tdConn = TMConnection(TDHost,
            #                      TDPort,
            #                      sslConnection=self.TMS.sslConnection,
            #                      keyfile=keyfile,
            #                      certfile=certfile,
            #                      ca_certs=ca_certs,
            #                      catchErrors=False,
            #                      loggerObj=logger)
            #
            #tdConn.sendAndClose(com)
            #
            logger.info("[%s] ... job %s has finished" % (threadName,jobID))


        #    info that TMMS has been started on host
        elif self.commands["TMMSSTARTED"].re.match(receivedStr):
            c = self.commands["TMMSSTARTED"]

            jsonInObj = c.re.match(receivedStr).groups()[0]
            jsonInObj = json.loads(jsonInObj)

            hostID = jsonInObj['hostID']
            hostName = jsonInObj['hostName']
            #tmmsPort = int(jsonInObj['tmmsPort'])
            #tmmsPid = jsonInObj['tmmsPid']
            #
            if hostID not in self.TMS.cluster:
                # add hServerProxy but do not check connection
                TMMS = hServerProxy( user = self.TMS.user,
                                     host = hostName,
                                     serverType = 'TMMS',
                                     sslConnection = self.TMS.sslConnection,
                                     keyfile = self.TMS.keyfile,
                                     certfile = self.TMS.certfile,
                                     ca_certs = self.TMS.ca_certs )

                self.TMS.cluster[hostID] = TMMS
            #    
            #self.TMS.cluster[hostName].setTMMSPort(tmmsPort)
            #self.TMS.cluster[hostName].setTMMSPid(tmmsPid)



        elif self.commands["SETALLPJOBSASWAITING"].re.match( receivedStr ):
            pJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                    db.JobDetails.job_status_id==self.TMS.databaseIDs['pending'] ) ).all()

            for job in pJobs:
                # free occupied slots from host
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==job.job_details.host_id ).\
                  update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - job.slots } )

                # set job as waiting
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: self.TMS.databaseIDs['waiting'] } )

                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = self.TMS.databaseIDs['waiting'] )

                dbconnection.introduce( jobHistory )

            dbconnection.commit()

        elif self.commands["SETALLPJOBSASFINISHED"].re.match( receivedStr ):
            pJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( and_(db.Job.user_id==self.TMS.userID,
                                                                                    db.JobDetails.job_status_id==self.TMS.databaseIDs['pending'] ) ).all()

            for job in pJobs:
                # free occupied slots from host
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==job.job_details.host_id ).\
                  update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - job.slots } )

                # set job as waiting
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: self.TMS.databaseIDs['finished'] } )

                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = self.TMS.databaseIDs['finished'] )

                dbconnection.introduce( jobHistory )

            dbconnection.commit()




        elif self.commands["LSHISTORY"].re.match(receivedStr):
            c = self.commands["LSHISTORY"]

            epoch = c.re.match(receivedStr).groups()[0]

            hist = history.getHistory(since=epoch)

            request.send(json.dumps(hist))
            
            
        #  termination is requested
        elif self.commands["SHUTDOWN"].re.match(receivedStr):
            #### kill all jobs
            ###
            ###hostList = {}	# {<hostName>: {'tmmsPort': <port>, 'jobIDs':[<jobID1>,...]}}
            #### loop over all jobs and assign jobIDs to hosts
            ###for jobID,job in self.TMS.jobsDict.iteritems():
            ###    # get host and port of TMMS
            ###    tmmsHost = job.getJobInfo('host')
            ###    tmmsPort = self.TMS.cluster[tmmsHost].tmmsPort
            ###
            ###    # append jobID to host
            ###    if tmmsHost in hostList:
            ###        hostList[tmmsHost]['jobIDs'].append(jobID)
            ###    else:
            ###        hostList[tmmsHost] = {'tmmsPort':tmmsPort, 'jobIDs':[jobID]}
            ###
            ###threadList = []
            ###for tmmshHost,l in hostList.iteritems():
            ###    tmmsPort = l['tmmsPort']
            ###    jobIDs = l['jobIDs']
            ###
            ###    current = killJobs(jobIDs,tmmsHost,tmmsPort,self.TMS)
            ###    threadList.append(current)
            ###    current.start()
            ###
            #### wait until all jobs on every tmms has been killed
            ###for t in threadList:
            ###    t.join()

            ## send deleting request to taskdispatcher
            #jobIDs = TMS.jobsDict.keys()
            #com = 'deletejobs:%s' % join(jobIDs,":")
            #
            #tdConn = TMConnection(TDHost,
            #                      TDPort,
            #                      sslConnection=TMS.sslConnection,
            #                      keyfile=keyfile,
            #                      certfile=certfile,
            #                      ca_certs=ca_certs,
            #                      catchErrors=True,
            #                      logFile=TMS.logFileTMS)
            #
            #tdConn.sendAndClose(com)

            request.send("terminating TMS ...")
            self.TMS.shutdownImmediatly = True


            #print "terminating ..."
            #self.TMS.shutdown()
            #print "... done terminating"

        ########
        # unknown command
        else:
            #if self.verboseMode:
            #    sys.stdout.write("TMS: What do you want?\n")
            logger.info("[%s] ... unkown command" % (threadName))
            request.send("TMS: What do you want?")



        # log processing time
        procTime2 = datetime.now()
        procTimeDt = procTime2-procTime1
        #procTimeSec = "%.5f" % (1.0*procTimeDt.microseconds/10**6 + procTimeDt.seconds + procTimeDt.days*24*3600)

        logger.info("[%s] ... done. processed in %ss." % (threadName,str(procTimeDt)))
        return persistentSocket,waitForNextRequest

    #def broadcastString(self,s, TMS):
    #    if TMS.verboseMode:
    #        sys.stdout.write("[%s] %s\n" %  (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),s) )
    #        sys.stdout.flush()
    #
    #    disconnectedIS = []
    #    for iSocket in TMS.infoSockets:
    #        try:
    #            iStr = "TMS: ["+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"] "+s+"\n"
    #            iSocket.send(iStr)
    #        except:
    #            disconnectedIS.append(iSocket)
    #
    #    # remove disconnectedIS
    #    for iSocket in disconnectedIS:
    #        TMS.infoSockets.remove(iSocket)

    #def logOutput(self,s):
    #    if self.TMS.logFileTMS:
    #        self.TMS.logFileTMS.write("[%s] %s\n" %  (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),s) )
    #        self.TMS.logFileTMS.flush()


    def formatJobList(self,jobList):
        """! return all jobs from jobList with jobID, jobinfo, and status
        
        @param jobList list of jobIDs which will be shown
        """

        jobs = ""
        for i,jobID in enumerate(jobList):
            jobs += "{i} - {id}\n".format(i=i, id=jobID )
            
            #job = self.TMS.jobsDict.get(jobID,None)
            #if job:
            #    jobInfo = job.getJobInfo('jobInfo')
            #    status = job.getStatus()
            #    #startTime = job.getStartTime()
            #    #startTime = startTime.replace(':','.')
            #    #endTime = job.getEndTime()
            #    #if endTime:
            #    #    endTime = endTime.replace(':','.')
            #
            #    #jobs += """%s - %s:%s:%s:%s:%s\n"""  % (i,jobID,jobInfo,startTime,endTime,status)
            #    jobs += """%s - %s:%s:%s\n"""  % (i,jobID,jobInfo,status)

        return jobs.strip()
    
    def formatJobOutput(self,jobList):
        """! return all jobs from jobList with jobID, jobinfo, and status
        @param jobList list of jobIDs which will be shown
        @param TMS TaskManagerServer instance
        """

        jobs = ""
        for (i,jobID) in enumerate(jobList):
            job = self.TMS.jobsDict.get(jobID,None)
            if job:
                jobInfo = job.getJobInfo('jobInfo')
                status = job.getStatus()
                #startTime = job.getStartTime()
                #startTime = startTime.replace(':','.')
                #endTime = job.getEndTime()
                #if endTime:
                #    endTime = endTime.replace(':','.')

                #jobs += """%s - %s:%s:%s:%s:%s\n"""  % (i,jobID,jobInfo,startTime,endTime,status)
                jobs += """%s - %s:%s:%s\n"""  % (i,jobID,jobInfo,status)

        return jobs.strip()

    def sentJob(self,jobID):
        """!sent job to host for execuation
        @param jobID id of job"""
        pass


    ##def executeJobEvent(self,request,threadName,jobID):
    ##    """!execute job events while considering correct order of sending events to taskdispatcher
    ##    @param request request from client
    ##    @param threadName name of thread handling connection to client
    ##    @param jobID id of job
    ##    """
    ##
    ##    status = self.TMS.jobsDict[jobID].getStatus()
    ##
    ##    if status == 3:
    ##        # job has already been added
    ##        # --> sent job to host for execution
    ##
    ##        hostName = self.TMS.jobsDict[jobID].getJobInfo('host')
    ##
    ##        command = self.TMS.jobsDict[jobID].getJobInfo('command')
    ##        shell = self.TMS.jobsDict[jobID].getJobInfo('shell')
    ##        logFile = self.TMS.jobsDict[jobID].getJobInfo('logFile')
    ##
    ##        rt = self.runJob(jobID,hostName,command,logFile,shell,self.TMS)
    ##
    ##        if rt:
    ##            # starting of job has failed
    ##            host = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherHost')
    ##            port = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherPort')
    ##
    ##            # send info to task dispatcher
    ##            com = 'ProcessStartFailed:%s:%s' % (jobID,rt)
    ##
    ##            tdConn = TMConnection(host,
    ##                                  port,
    ##                                  sslConnection=self.TMS.sslConnection,
    ##                                  keyfile=keyfile,
    ##                                  certfile=certfile,
    ##                                  ca_certs=ca_certs,
    ##                                  catchErrors=False,
    ##                                  loggerObj=logger)
    ##
    ##            tdConn.sendAndClose(com)
    ##
    ##            logger.info("[%s] ... failed [%s] with return code [%s]" % (threadName,jobID,rt))
    ##
    ##            # set job status to 1 (same as added)
    ##            self.TMS.jobsDict[jobID].setAsStartFailed()
    ##
    ##        else:
    ##            tmmsPort = self.TMS.cluster[hostName].TMMSPort
    ##            logger.info("[%s] ... job %s has been successfully sent to %s:%s" % (threadName,jobID,hostName,tmmsPort))
    ##
    ##    elif status == 7:
    ##        # job has already been added, sent to host, and started on host
    ##        # --> send info to taskdispatcher
    ##        hostName = self.TMS.jobsDict[jobID].getJobInfo('host')
    ##
    ##        host = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherHost')
    ##        port = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherPort')
    ##        pid = self.TMS.jobsDict[jobID].getJobInfo('pid')
    ##        fileCommand = self.TMS.jobsDict[jobID].getJobInfo('fileCommand')
    ##        fileOutput = self.TMS.jobsDict[jobID].getJobInfo('fileOutput')
    ##        fileError = self.TMS.jobsDict[jobID].getJobInfo('fileError')
    ##
    ##        jsonObj = {'jobID': jobID,
    ##                   'pid': pid,
    ##                   'fileCommand': fileCommand,
    ##                   'fileOutput': fileOutput,
    ##                   'fileError': fileError}
    ##
    ##        jsonObj = json.dumps(jsonObj)
    ##
    ##        com = "ProcessStarted:%s" % (jsonObj)
    ##
    ##        tdConn = TMConnection(host,
    ##                              port,
    ##                              sslConnection=self.TMS.sslConnection,
    ##                              keyfile=keyfile,
    ##                              certfile=certfile,
    ##                              ca_certs=ca_certs,
    ##                              catchErrors=False,
    ##                              loggerObj=logger)
    ##
    ##        tdConn.sendAndClose(com)
    ##
    ##        self.TMS.runningJobs.append(jobID)
    ##
    ##        tmmsPort = self.TMS.cluster[hostName].TMMSPort
    ##
    ##        logger.info("[%s] ... job %s has been started by TMMS %s:%s" % (threadName,jobID,hostName,tmmsPort))
    ##
    ##    elif status == 15:
    ##        # job has already been added, sent to host, started, and finished
    ##        # --> send info to taskdispatcher
    ##
    ##        host = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherHost')
    ##        port = self.TMS.jobsDict[jobID].getJobInfo('TaskDispatcherPort')
    ##        returnCode = self.TMS.jobsDict[jobID].getJobInfo('returnCode')
    ##
    ##        com = "ProcessFinished:%s:%s" % (jobID, returnCode)
    ##
    ##        tdConn = TMConnection(host,
    ##                              port,
    ##                              sslConnection=self.TMS.sslConnection,
    ##                              keyfile=keyfile,
    ##                              certfile=certfile,
    ##                              ca_certs=ca_certs,
    ##                              catchErrors=False,
    ##                              loggerObj=logger)
    ##
    ##        tdConn.sendAndClose(com)
    ##
    ##        try:
    ##            self.TMS.runningJobs.remove(jobID)
    ##        except:
    ##            pass
    ##
    ##        try:
    ##            del self.TMS.jobsDict[jobID]
    ##        except:
    ##            pass
    ##
    ##        logger.info("[%s] ... job %s has been finished" % (threadName,jobID))


    #def runJob(self,jobID):
    #    """! @brief send job to TMMS on host.
    #
    #    @param jobID job id
    #    """
    #    #currThread = threading.currentThread()
    #    #threadName = currThread.getName()
    #
    #    job = self.TMS.jobsDict[jobID]
    #    if job:
    #        ## name of host on which job is supposed to be executed
    #        hostName = job.getJobInfo("host")
    #
    #        ## host on which job is supposed to be executed
    #        host = self.TMS.cluster[hostName]
    #
    #        ## establish connection to TMMS
    #        tmmsConn = host.connectToTMMS()
    #
    #        # send job to to TMMS
    #        if host.tmmsIsRunning:
    #            # job infos
    #            command = job.getJobInfo("command")
    #            logFile = job.getJobInfo("logFile")
    #            shell = job.getJobInfo("shell")
    #
    #            jsonOutObj =  { 'jobID': jobID,
    #                            'command': command,
    #                            'logFile': logFile,
    #                            'shell': shell}
    #
    #            jsonOutObj = json.dumps(jsonOutObj)
    #
    #            # request for start job on host
    #            com = "runjob:%s" % jsonOutObj
    #            tmmsConn.sendAndClose(com)
    #            self.TMS.setJobAs(jobID,"pending")

    def runJob(self,jobID, hostName):
        """! @brief send job to TMMS on host.

        @param jobID (int) job id
        @param hostName (string) name of host on which job is supposed to be executed
        """
        #currThread = threading.currentThread()
        #threadName = currThread.getName()

        hostName = job.getJobInfo("host")

        ## ClusterHost instance, representing host on which job is supposed to be executed
        host = self.TMS.cluster[hostName]

        ## establish connection to TMMS
        tmmsConn = host.connectToTMMS()

        # send job to to TMMS
        if host.tmmsIsRunning:
            jsonOutObj =  { 'jobID': jobID }
            jsonOutObj = json.dumps(jsonOutObj)

            # request for start job on host
            com = "runjob:%s" % jsonOutObj
            
            tmmsConn.sendAndClose(com)


    def formatJobList(self,jobList):
        """ format job list for output """

        # sort???
        if jobList:
            jobList = map(lambda (i,e): "%s - %s" % (i,e), enumerate(jobList))
            jobList = join(jobList,'\n')
        else:
            jobList = "no jobs found."

        return jobList

    def suspendJob(self,computer,pid):
        if pid:
            job="ssh -x -a %s pstree -p %s" % (computer,pid)
            rePIDS=re.compile('\(\d+\)')
            output=os.popen(job)
            while 1:
                line = output.readline()
                for m in re.finditer(rePIDS,line):
                    pid1=m.group().replace('(','')
                    pid1=pid1.replace(')','')
                    j1="ssh -x -a %s kill -STOP %s" %(computer,pid1)
                    subprocess.Popen(j1,shell=True)
                if not line: break

    def resumeJob(self,computer,pid):
        if pid:
            job="ssh -x -a %s pstree -p %s" % (computer,pid)
            rePIDS=re.compile('\(\d+\)')
            output=os.popen(job)
            while 1:
                line = output.readline()
                for m in re.finditer(rePIDS,line):
                    pid1=m.group().replace('(','')
                    pid1=pid1.replace(')','')
                    j1="ssh -x -a %s kill -CONT %s" % (computer,pid1)
                    subprocess.Popen(j1,shell=True)
                if not line: break

    def killJob(self,jobID,TMS):
        """kill job with jobID by sending request to TMMS"""
        if jobID not in TMS.jobsDict:
            logger.info("unkown job to kill: %s" % (jobID))
            return -1

        pid = TMS.jobsDict[jobID].getJobInfo('pid')

        # get host and port of TMMS
        tmmsHost = TMS.jobsDict[jobID].getJobInfo('host')
        tmmsPort = TMS.cluster[tmmsHost].tmmsPort

        # connect to TMMS and send kill signal
        tmmsConn = TMConnection(tmmsHost,
                                tmmsPort,
                                sslConnection=TMS.sslConnection,
                                keyfile=keyfile,
                                certfile=certfile,
                                ca_certs=ca_certs,
                                catchErrors=False,
                                loggerObj=logger)

        com = "killjob:%s" % pid
        if tmmsConn.openConnection:
            tmmsConn.sendAndRecvAndClose(com)
            if tmmsConn.requestSent:
                return tmmsConn.response
            else:
                return "Killing job has failed!"
        else:
            # do something
            return "Killing job has failed!"



    def terminateAllJobs(self,TMS):
        jobIDs = TMS.jobsDict.keys()
        for jobID in jobIDs:
            computer = TMS.jobsDict[jobID].getJobInfo('host')
            self.terminateJob(jobID,TMS)

        TMS.jobsDict = {}



    def killSuspendedJob(self,jobID):
        try:
            del TMS.jobsDict[jobID]
            self.jobs.remove(jobID)
        except:
            pass




##progName = "TMS.py"
##def printHelp(where=sys.stdout):
##    where.write("NAME              %s - taskmanager server\n" % progName)
##    where.write("\n")
##    where.write("SYNOPSIS          %s -h\n" % progName)
##    where.write("                  %s [OPTION] Port\n" % progName)
##    where.write("\n")
##    where.write("DESCRIPTION       Starts a taskmanager server on PORT.\n")
##    where.write("\n")
##    where.write("OPTIONS\n")
##    where.write("   -d             Run TMS in non-daemon mode.\n")
##    where.write("   -h             Print this help.\n")
##    where.write("   -l LOGFILE     Write in and out communications in LOGFILE.\n")
##    where.write("   -s             Do not shutdown TMS after jobs has been processed.\n")
##    where.write("   -v             Verbose mode. Print status information on stdout.\n")
##    where.write("\n")
##    where.write("AUTHOR            Written by hotbdesiato.\n")
##
##
##
##
##
##if __name__ == '__main__':
##    textWidth = 80
##    parser = argparse.ArgumentParser(
##        prog="hTMConnect",
##        formatter_class=argparse.RawDescriptionHelpFormatter,
##        usage="%(prog)s [-h --help] [options] COMMAND",
##        description='\n'.join( textwrap.wrap("Connect to a server, send the COMMAND to the server and print response to stdout. Unless host and port are specified with option -S, use the following", width=textWidth) +
##                               ['\n'] +
##                               textwrap.wrap("  host: {}".format(tdHost), width=textWidth)+
##                               textwrap.wrap("  port: {}".format(tdPort), width=textWidth)
##                               ),
##        epilog='Written by Hendrik.')
##    parser.add_argument('command',
##                        metavar = 'COMMAND',
##                        help = "Command which will be sent to the server." )    
##    
##
##    
##    try:
##        opts,args = getopt.getopt(sys.argv[1:],"hdl:sT:v")
##    except getopt.error, message:
##        sys.stderr.write('%s: Error!! %s\n' % (sys.argv[0].split('/')[-1],message) )
##        printHelp(sys.stderr)
##        sys.exit(-1)
##
##    runNotAsDaemon = False
##    persistent = False
##    verboseMode = False
##    logFileTMS = None
##    for option, param in opts:
##        if option == '-h':
##            printHelp(sys.stdout)
##            sys.exit(0)
##        elif option == '-d':
##            runNotAsDaemon = True
##        elif option == "-l":
##            try:
##                # create console handler and configure
##                fileLog = logging.FileHandler(filename="/home/hhache/tmp/logger.log",mode='w')
##                fileLog.setLevel(logging.DEBUG)
##                fileLog.setFormatter(formatter)
##
##                # add handler to logger
##                logger.addHandler(fileLog)
##
##                #logFileTMS = open(param,'w')
##            except IOError,msg:
##                sys.stderr.write("TMS ERROR: %s\n" % msg)
##                sys.stderr.write("TMS: logfile is ignored\n")
##
##        #elif option == "-T":
##        #    TaskDispatcherHost,TaskDispatcherPort = param.split(":")
##        elif option == "-s":
##            persistent = True
##        elif option == '-v':
##            logger.setLevel(logging.DEBUG)
##            verboseMode = True
##
##    if len(args)==0:
##        sys.stderr.write('%s: Error!! %s\n' % (sys.argv[0].split('/')[-1],"Port number is not given!") )
##        printHelp(sys.stderr)
##        sys.exit(-1)
##    elif len(args)>1:
##        sys.stderr.write('%s: Error!! %s\n' % (sys.argv[0].split('/')[-1],"Too many arguments!") )
##        printHelp(sys.stderr)
##        sys.exit(-1)
##
##    TMS = None
##    try:
##        port = int(args[0])
##        host = uname()[1]
##
##        TMS=TaskManagerServer(host=host,
##                              port=port,
##                              sslConnection=True,
##                              keyfile=keyfile,
##                              certfile=certfile,
##                              ca_certs=ca_certs,
##                              handler=TaskManagerServerHandler,
##                              processor=TaskManagerServerProcessor(),
##                              verboseMode=verboseMode,
##                              persistent=persistent,
##                              logFileTMS=logFileTMS)
##
##        if runNotAsDaemon:
##            TMS.run() # run not as daemen; for debugging
##        else:
##            TMS.start() # run as deamon
##
##        try:
##            terminateAllTMMS(TMS)
##        except:
##            sys.stderr.write("%s\n" % sys.exc_info())
##            sys.stderr.write("Termination of all TMMS has failed!\n")
##
##    except exceptions.KeyboardInterrupt:
##        try:
##            terminateAllTMMS(TMS)
##        except:
##            sys.stderr.write("Termination of all TMMS has failed!\n")
##
##        sys.stderr.write("TMS KeyboardInterrupt")
##        sys.stderr.write("TMS shut down.\n")
##    #except socket.error,msg:
##    #    try:
##    #        terminateAllTMMS(TMS)
##    #    except:
##    #        sys.stderr.write("Termination of all TMMS has failed!\n")
##    #
##    #    sys.stderr.write("TMS (%s:%s) Socket Error: %s\n" % (host,port,msg))
##    #    sys.stderr.write("TMS shut down.\n")
##    except exceptions.SystemExit,msg:
##        try:
##            terminateAllTMMS(TMS)
##        except:
##            sys.stderr.write("Termination of all TMMS has failed!\n")
##
##        sys.stderr.write("TMS System Exit.\n")
##        sys.stderr.write("TMS shut down.\n")
##        sys.stderr.flush()
##        sys.stdout.flush()
##    except:
##        if TMS:
##            try:
##                terminateAllTMMS(TMS)
##            except:
##                sys.stderr.write("Termination of all TMMS has failed!\n")
##
##        sys.stderr.write("TMS Error: %s\n" % sys.exc_info()[1])
##        sys.stderr.write("TRACEBACK:")
##        traceback.print_exc(file=sys.stderr)
##        #raise
##    finally:
##        logging.shutdown()
