# user specific taskmanager menial server (TMMS) which is dynamically started by TMS on each host of cluster

import socket
import SocketServer
import select
import re
import subprocess
import pwd
import sys
import getopt
from datetime import datetime
from time import time, strftime,sleep
#from random import choice
from string import join,replace
import pickle
import string
#from copy import copy,deepcopy
import os
from daemon import Daemon
import traceback
#import xml.dom.minidom
import exceptions
import json
import tempfile
import logging
import threading

homedir = os.environ['HOME']
user = pwd.getpwuid(os.getuid())[0]


# logging

logger = logging.getLogger('TMMS')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('[%(asctime)-15s] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.INFO)
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)

# create console handler and configure
fileLog = logging.FileHandler(filename="/tmp/TMMS.%s.log" % user,mode='w')
fileLog.setLevel(logging.DEBUG)
fileLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(fileLog)

# get path to taskmanager. it is assumed that this script is in the lib directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..' ) )

#binPath    = '%s/bin' % tmpath	# for hRun
#serverPath = '%s/UserServer' % tmpath
#etcPath    = '%s/etc'    % tmpath	# for TaskDispatcher.info
#libPath  = '%s/lib' % tmpath		# for hSocket

# ssl configuration
certfile = "%s/.taskmanager/%s.crt" % (homedir,user)
keyfile = "%s/.taskmanager/%s.key" % (homedir,user)
ca_certs = "%s/.taskmanager/ca_certs.%s.crt" % (homedir,user)


#sys.path.insert(0,libPath)


from hSocket import hSocket
from hDBConnection import hDBConnection

import hDatabase as db

##progName = "TMMS.py"
##def printHelp(where=sys.stdout):
##    where.write("NAME              %s - taskmanager menial server\n" % progName)
##    where.write("\n")
##    where.write("SYNOPSIS          %s -h\n" % progName)
##    where.write("                  %s [OPTION] Port TMSHost TMSPort\n" % progName)
##    where.write("\n")
##    where.write("DESCRIPTION       Starts a taskmanager menial server on PORT. Communicate with\n")
##    where.write("                  TaskManagerServer on TMSHost:TMSPort.\n")
##    where.write("\n")
##    where.write("OPTIONS\n")
##    where.write("   -d             Run TMMS in non-daemon mode.\n")
##    where.write("   -h             Print this help.\n")
##    where.write("   -l LOGFILE     Write in and out communications in LOGFILE.\n")
##    where.write("   -v             Verbose mode. Print status information on stdout.\n")
##    where.write("\n")
##    where.write("AUTHOR            Written by hotbdesiato.\n")


# TaskManagerMenial Server is a TCPServer using the forking mix in to create a new process for every request. SocketServer.ForkingMixIn
class TaskManagerMenialServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer, Daemon):
    # This means the main server will not do the equivalent of a
    # pthread_join() on the new threads.  With this set, Ctrl-C will
    # kill the server reliably.
    daemon_threads = True

    # By setting this it is allowed for the server to re-bind to the address by
    # setting SO_REUSEADDR, meaning you don't have to wait for
    # timeouts when you kill the server and the sockets don't get
    # closed down correctly.
    allow_reuse_address = True
    request_queue_size = 10

    # set maximum number of children. actually has to be adjust according to number of cpu's
    max_children=64

    def __init__(self,
                 port,
                 tmsHost,
                 tmsPort,
                 handler,
                 processor,
                 EOCString=None,
                 sslConnection=False,
                 keyfile=None,
                 certfile=None,
                 ca_certs=None,
                 verboseMode=False,
                 logFileTMMS=None):

        try:
            os.remove('/tmp/TMMS.%s.pid' % user)
        except:
            pass

        Daemon.__init__(self,'/tmp/TMMS.%s.pid' % user)
        
        self.host=os.uname()[1]
        self.port=port
        self.startTime = strftime("%d %b %Y, %H.%M.%S") # strftime("%Y %m %d %H.%M.%S")
        self.ID = int(time())
        self.tmsHost=tmsHost
        self.tmsPort=tmsPort
        self.user=user

        self.EOCString=EOCString
        self.sslConnection=sslConnection
        self.keyfile=keyfile
        self.certfile=certfile
        self.ca_certs=ca_certs

        self.processor = processor

        #self.infoSockets = []

        self.verboseMode = verboseMode
        self.logFileTMMS = logFileTMMS

        # db connection
        dbconnection = hDBConnection()

        # get database id of host
        try:
            self.hostID=dbconnection.query( db.Host.id ).filter( db.Host.full_name==self.host ).one()[0]
        except:
            sys.stderr.write( "Host is not known by TaskManager!" )
            sys.exit(-1)

        # save database ids of some entries in self.databaseIDs
        self.databaseIDs = dict( dbconnection.query( db.JobStatus.name, db.JobStatus.id ).all() )

        dbconnection.remove()
        
        # start the server
        SocketServer.TCPServer.__init__(self,(self.host,self.port), handler)

    def run(self):
        """
        Start up a server; each time a new
        request comes in it will be handled by a RequestHandler class
        """
        # get pid of daemon
        try:
            with open(self.pidfile) as f:
                self.pid = f.readline().strip()
        except:
            self.pid = os.getpid()

        try:
            # save host and port and other info
            tmsConfig = hTaskManagerMenialServerInfo( self.host )
            tmsConfig.save([ ('host', self.host),
                             ('port', self.port),
                             ('sslconnection', self.sslConnection),
                             ('eocstring', self.EOCString),
                             ('laststart', self.startTime),
                             ('pid', self.pid)
                             ])
        except:
            pass

        logger.info("")
        logger.info('TaskManagerMenialServer has been started on %s:%s [pid %s]' % (self.host,self.port,self.pid))
        logger.info("")

        
        # send info to TMS
        try:
            jsonOutObj = {'hostID': self.hostID,
                          'hostName': self.host,
                          'tmmsPort': self.port,
                          'tmmsPid': self.pid}

            jsonOutObj = json.dumps(jsonOutObj)

            clientSock = hSocket(host=self.tmsHost,
                                 port=self.tmsPort,
                                 EOCString=self.EOCString,
                                 sslConnection=self.sslConnection,
                                 certfile=certfile,
                                 keyfile=keyfile,
                                 ca_certs=ca_certs,
                                 catchErrors=False)

            clientSock.send("TMMSStarted:%s" % jsonOutObj)
            clientSock.close()
        except:
            logger.error("TMMS: Connection to TMS %s:%s failed!" % (self.tmsHost,self.tmsPort))
            sys.exit(-1)


        try:
            self.serve_forever()

            print "... done"

        except KeyboardInterrupt:
            sys.exit(0)
        finally:
            pass




# The RequestHandler handles an incoming request.
class TaskManagerMenialServerHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, clientAddress, TMMS):
        self.request = request
        self.requestHost,self.requestPort = self.request.getpeername()
        self.TMMS = TMMS

        SocketServer.BaseRequestHandler.__init__(self, request, clientAddress, self.TMMS)

    #def finish(self):
    #    pass

    def handle(self):
        logger.info("[%s:%s] got new request from %s:%s" % (self.TMMS.host,
                                                            self.TMMS.port,
                                                            self.requestHost,
                                                            self.requestPort))


        # waits for event on request socket
        #(sread, swrite, sexc) = select.select([self.request], [], [], None)

        hSock = hSocket(sock=self.request,
                        EOCString=self.TMMS.EOCString,
                        serverSideSSLConn=True,
                        sslConnection=self.TMMS.sslConnection,
                        certfile=self.TMMS.certfile,
                        keyfile=self.TMMS.keyfile,
                        ca_certs=self.TMMS.ca_certs)

        receivedStr = hSock.recv()

        try:
            self.TMMS.processor.process(receivedStr, hSock, self.TMMS)
        except:
            # processing failed
            tb = sys.exc_info()

            logger.error('Error while processing request from %s:%s!\n' % (self.requestHost,
                                                                          self.requestPort))

            traceback.print_exception(*tb,file=sys.stderr)

            hSock.send("Error while processing request!\n%s" %  tb[1])

        logger.info("[%s:%s] ... done" % (self.requestHost,self.requestPort))

        # closing socket
        try:
            hSock.close()
        except:
            pass

        del hSock



class TaskManagerMenialServerProcessor:
    def process(self, s, request, TMMS):
        """process request string"""

        h,p = request.socket.getpeername()
        currThread = threading.currentThread()
        threadName = currThread.getName()

        pid = os.getpid()

        receivedStr = s

        #dbconnection = hDBConnection( TMMS.dbconnection.ScopedSession )
        
        self.outputMsg('[%s] IN: %s' % (threadName,s),TMMS)

        if not receivedStr:
            self.outputMsg('[%s] ... socket has been closed' % threadName,TMMS)
            return False, False

        #####################
        # check if request is a known command

        #######
        # help
        #    get help
        elif re.match("^help$",receivedStr):
            h = []
            h.append("Commands:")
            h.append("  job commands:")
            h.append("    runjob       run job on host.")
            h.append("    killjobs:<PID>[:<PID>...]")
            h.append("                 kill jobs with pids.")
            h.append("")
            h.append("  server commands:")
            h.append("    ping         ping server.")
            h.append("    gettmsinfo   get task manager server info.")
            h.append("    gettmmsinfo  get task manager menial server info.")
            h.append("    shutdown     shutdown of TMMS is requested.")

            request.send(join(h,'\n'))

        ########
        # check
        #    response an ok
        if re.match('^ping$',receivedStr):
            request.send("pong")
            processingStatus = "done"

        ########
        # gettmsinfo
        #    get info about tms
        elif re.match('^gettmsinfo$',receivedStr):
            try:
                clientSock = hSocket(host=TMMS.tmsHost,
                                     port=TMMS.tmsPort,
                                     EOCString=TMMS.EOCString,
                                     sslConnection=TMMS.sslConnection,
                                     certfile=certfile,
                                     keyfile=keyfile,
                                     ca_certs=ca_certs,
                                     catchErrors=False)

                clientSock.send("check")
                recv = clientSock.recv()
                clientSock.close()

                resp = []
                resp.append("Information about connected TMS:")
                resp.append("  Host: %s" % TMMS.tmsHost)
                resp.append("  Port: %s" % TMMS.tmsPort)
                if recv=="ok":
                    resp.append("  Status: ok")
                else:
                    resp.append("  Status: Connection failed!")
                request.send(join(resp,"\n"))
            except:
                resp = []
                resp.append("Information about connected TMS:")
                resp.append("  Host: %s" % TMMS.tmsHost)
                resp.append("  Port: %s" % TMMS.tmsPort)
                resp.append("  Status: Connection failed!")
                request.send(join(resp,"\n"))

            processingStatus = "done"


        ########
        # runjob
        #    run job on host and monitor
        elif re.match('^runjob:(.*)$',receivedStr):
            jsonInObj = re.match('runjob:(.*)$',receivedStr).groups()[0]
            jsonInObj = json.loads(jsonInObj)

            jobID = jsonInObj['jobID']
            
            ##command = jsonInObj['command']
            ##logFile = jsonInObj.get('logFile',None)
            ##shell = jsonInObj['shell']

            # connect to database
            dbconnection = hDBConnection()

            # get job instance
            job = dbconnection.query( db.Job ).get( jobID )

            command = job.command
            stdout = job.stdout
            stderr = job.stderr
            logFile = job.logfile
            shell = job.shell
            
            # create temporary file object for command which is executed
            fCom = tempfile.NamedTemporaryFile(prefix="tmms.",bufsize=0,delete=False)
            fCom.write("%s\n" % command)
            fCom.close()

            # create temporary file object for stdout and stderr of executing command
            fOut = tempfile.NamedTemporaryFile(prefix="tmms.",bufsize=0,delete=False)
            fErr = tempfile.NamedTemporaryFile(prefix="tmms.",bufsize=0,delete=False)

            startTime = datetime.now()

            self.outputMsg('[{th}] [job {j}] job has been started at {t}'.format(th=threadName,j=jobID,t=str(startTime)),TMMS)

            # execute job in a subprocess
            sp = subprocess.Popen(command,
                                  shell=True,
                                  executable=shell,
                                  stdout=fOut,
                                  stderr=fErr)

            # store info about running job in database
            
            # set job as running
            dbconnection.query( db.JobDetails.job_id ).\
              filter( db.JobDetails.job_id==jobID ).\
              update( {db.JobDetails.job_status_id: TMMS.databaseIDs['running'] } )
              
            job.job_details.host_id = TMMS.hostID
            job.job_details.pid = sp.pid

            # set history
            jobHistory = db.JobHistory( job=job,
                                        job_status_id = TMMS.databaseIDs['running'] )
            
            dbconnection.introduce( jobHistory )
            dbconnection.commit()
            dbconnection.remove()
            
            ##jsonOutObj = {'jobID': jobID,
            ##              'pid': sp.pid,
            ##              'fileCommand': fCom.name,
            ##              'fileOutput': fOut.name,
            ##              'fileError': fErr.name}
            ##
            ##jsonOutObj = json.dumps(jsonOutObj)
            ##
            ##
            ### send info to TMS
            ##try:
            ##    clientSock = hSocket(host=TMMS.tmsHost,
            ##                         port=TMMS.tmsPort,
            ##                         EOCString=TMMS.EOCString,
            ##                         sslConnection=True,
            ##                         certfile=certfile,
            ##                         keyfile=keyfile,
            ##                         ca_certs=ca_certs,
            ##                         catchErrors=False)
            ##
            ##    clientSock.send("ProcessStarted:%s" % jsonOutObj)
            ##    clientSock.close()
            ##except:
            ##    print sys.exc_info()[1]
            ##    sys.stderr.write("TMMS: Connection to TMS failed. Job %s is terminated." % jobID)
            ##    # write to log file
            ##    #self.killJob()

            # wait until process has finished
            sp.wait()

            endTime = datetime.now()

            self.outputMsg('[{th}] [{j}] job has been finished at {t}'.format(th=threadName,j=jobID,t=str(endTime)),TMMS)

            # write command, stdout, and stderr to a logfile
            if logFile:
                f = open('%s' % logFile,'w')

                f.write("-----------------------\n")
                f.write("--------command--------\n")
                f.write("-----------------------\n")
                f.write("%s\n" %command)
                f.write("\n")
                f.write("-----------------------\n")
                f.write("----------info---------\n")
                f.write("-----------------------\n")
                f.write("host: {0}\n".format(os.uname()[1]))
                f.write("started: %s\n" % (startTime))
                f.write("finished: %s\n" % (endTime))
                f.write("\n")

                f.write("-----------------------\n")
                f.write("------BEGIN stdout-----\n")
                f.write("-----------------------\n")

                fOut.seek(0)

                for line in fOut:
                    f.write("%s" % line)

                f.write("-----------------------\n")
                f.write("------END stdout-------\n")
                f.write("-----------------------\n")
                
                f.write("\n")
                
                f.write("-----------------------\n")
                f.write("------BEGIN stderr-----\n")
                f.write("-----------------------\n")

                fErr.seek(0)
                for line in fErr:
                    f.write("%s" % line)

                f.write("-----------------------\n")
                f.write("------END stderr-------\n")
                f.write("-----------------------\n")

                f.close()

            fOut.close()
            fErr.close()

            # connect to database
            dbconnection = hDBConnection()

            # get job instance (again, since we use here another connection)
            job = dbconnection.query( db.Job ).get( jobID )
            
            # set job as finished
            dbconnection.query( db.JobDetails.job_id ).\
              filter( db.JobDetails.job_id==jobID ).\
              update( {db.JobDetails.job_status_id: TMMS.databaseIDs['finished'] } )
            
            job.job_details.return_code = sp.returncode

            # set history
            jobHistory = db.JobHistory( job=job,
                                        job_status_id = TMMS.databaseIDs['finished'] )
            
            dbconnection.introduce( jobHistory )
            dbconnection.commit()
            dbconnection.remove()
            
            try:
                clientSock = hSocket(host=TMMS.tmsHost,
                                     port=TMMS.tmsPort,
                                     EOCString=TMMS.EOCString,
                                     sslConnection=TMMS.sslConnection,
                                     certfile=certfile,
                                     keyfile=keyfile,
                                     ca_certs=ca_certs,
                                     catchErrors=False)
            
                clientSock.send("ProcessFinished:%s" % (jobID))
                clientSock.close()
            except:
                sys.stderr.write("TMMS: Connection to TMS failed. ProcessFinish could not be send for job %s!" % jobID)
                # write to log file

            processingStatus = "done"


        ########
        # gettmmsinfo
        #    get task manager menial server info
        elif re.match('^gettmmsinfo$',receivedStr):
            resp = []
            resp.append("Information about TMMS:")
            resp.append("  Host: %s" % TMMS.host)
            resp.append("  Port: %s" % TMMS.port)
            resp.append("  User: %s" % TMMS.user)
            resp.append("  Start time: %s" % TMMS.startTime)
            #resp.append("  Number of threads: %s" % threading.activeCount())
            resp.append("  ")
            request.send(join(resp,"\n"))

            processingStatus = "done"


        ########
        # ????? only jobID
        # suspendjob:<hostName>:<pid>
        #    suspend a certain job
        elif re.match('suspendjob:(.*):(.*)', receivedStr):
            computer,PID = re.match('suspendjob:(.*):(.*)', receivedStr).groups()
            self.suspendJob(computer,PID)

            # change status of job

        ########
        # ????? only jobID
        # resumejob:<hostName>:<pid>
        #    resume a certain job
        elif re.match('resumejob:(.*):(.*)',receivedStr):
            computer,PID = re.match('resumejob:(.*):(.*)', receivedStr).groups()
            self.resumeJob(computer,PID)

            # change status of job


        ########
        # killjobs:<PID>[:<PID>...]
        #    kill processes
        elif re.match('^killjobs:(.*)$',receivedStr):
            pids = re.match('^killjobs:(.*)$',receivedStr).groups()[0]
            pidsList = pids.split(':')

            jobList = []
            for pid in pidsList:
                current = killJob(pid)
                jobList.append(current)
                current.start()

            # wait until all jobs on every tmms has been killed
            for j in jobList:
                j.join()

            request.send("Job(s) have been killed.")


        ########
        # shutdown
        #    termination is requested
        elif re.match('^shutdown$',receivedStr):
            # kill all running processes and shutdown server
            TMMS.shutdown()
            pass


        ########
        # unknown command
        else:
            #if self.verboseMode:
            #    sys.stdout.write("TMS: What do you want?\n")
            request.send("TMMS: What do you want?")


        return
        #return persistentSocket,terminateLater

    def outputMsg(self,s, TMMS):
        logger.info("%s" % s)

        if TMMS.logFileTMMS:
            TMMS.logFileTMMS.write("[%s] %s\n" %  (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),s) )
            TMMS.logFileTMMS.flush()


    # ???
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

    # ???
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

    # ???
    def terminateJob(self,jobID,TMS):
        pass

    # ???
    def terminateAllJobs(self,TMMS):
        pass

    # ???
    def killSuspendedJob(self,jobID):
        pass


class killJob( threading.Thread):
    def __init__(self,pid):
        self.pid = pid
        threading.Thread.__init__(self)

    def run(self):
        """ send kill command to tmms and wait """
        try:
            # kill all processes assocciated to pid
            cmd = """pstree -A -p %s | sed 's/(/\\n(/g' | grep '(' | sed 's/(\(.*\)).*/\\1/' | tr "\\n" " " """% self.pid
            sp = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
            out,err = sp.communicate()

            allpids = out
            if allpids:
                cmd = "kill -9 %s" % allpids
                sp = subprocess.Popen(cmd,shell=True)
                sp.wait()

        except:
            # do something?
            traceback.print_exc(file=sys.stderr)
            pass


