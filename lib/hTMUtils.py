import textwrap
from threading import Thread
import os
import re
import socket
import datetime
import tempfile
import subprocess

def renderHelp(commandNames,commandsList):
    """! @brief format help for output. loop over each name in commandNames and add formatted help to list"""
    h = []	# help list for output

    for com in commandNames:
        if com not in commandNames: next

        c = commandsList[com]

        # commmand itself
        l = "    "+c.command_name

        # add arguments:
        if c.arguments:
            l += ":"+c.arguments

        # add help text
        t = textwrap.wrap(c.help,64)
        if len(l)>15:
            # command+arguments are too long, start printing the help below
            h.append(l)
            t = map(lambda e: " "*16+e, t)
            h.extend(t)
        else:
            # start printing the help on same line as command
            l += " "*(16-len(l))
            l += t.pop(0)
            h.append(l)

            t = map(lambda e: " "*16+e, t)
            h.extend(t)

    return h


## @brief Try to ping a host 
class hPingHost(Thread):
    def __init__ (self,host):
        """! @brief constructor

        @param host (string) pingable host name
        """
        Thread.__init__(self)
        self.host = host
        self.status = (0,0)
        
    def run(self):
        """! @brief send a single ping to host, wati for at most one second and catch response"""
        #pingaling = os.popen("ping -q -r -c2 "+self.host,"r")
        pingaling = os.popen("ping -q -r -c1 -w1 "+self.host, "r")

        reTransmitted = re.compile(r"(\d) (?:packets )?transmitted")
        reReceived = re.compile(r"(\d) (?:packets )?received")
        
        for line in pingaling:
            transmitted = re.findall( reTransmitted, line)
            received = re.findall( reReceived, line)
        
            if received:
                self.status = ( int(transmitted[0]), int(received[0]) )
                
                break
            


class hHostLoad(Thread):
    def __init__(self, host, isPort=None ):
        Thread.__init__(self)
        
        self.host = host			# host for getting load
        self.ISPort = isPort		# InfoServer port on host
        
        self.load = None			# load of host
        self.isISRunning = False	# running status of InfoServer on host

    def run(self):
        """! @brief get load of host"""

        self.load = self.getLoad()

        cnt=0
        while not self.load and cnt<3:
            if self.ISPort: 
                self.ISPort += 1
                self.load = self.getLoad()
            else:
                break

            
        ##if self.ISPort:
        ##    # try to connect to InfoServer
        ##    clientSock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        ##    try:
        ##        clientSock.connect( (self.hostName, self.ISPort) )
        ##        clientSock.settimeout(1.0)
        ##        clientSock.send( "getload" )
        ##        self.load = clientSock.recv(64)
        ##        clientSock.close()
        ##
        ##        self.ISRunning = True
        ##        return
        ##    except socket.timeout:
        ##        print "[{t}] ERROR: Timeout on {h}:{p}".format(t=str(datetime.now()),h=self.hostName, p=self.ISPort )
        ##
        ##        return
        ##
        ##    except socket.error:
        ##        pass
        ##        #self.port += 1
        ##        #isPort = None
        ##
        ##    # ping host
        ##    p = pingHost(self.hostName)
        ##    p.start()
        ##    p.join()
        ##
        ##    # check status
        ##    if p.status[1]==0:	# p.status: (transmitted,received)
        ##        # unreachable
        ##        return
        ##
        ##    # try to start new InfoServer
        ##    cnt = 0
        ##    while(cnt!=10):
        ##        cnt+=1
        ##
        ##        #print "ISPort %s:%s" % (self.hostName,self.ISPort)
        ##        # start InfoServer
        ##        try:
        ##            com = 'ssh -x -a %s "python %s/InfoServer.py %s"' %(self.hostName, serverpath, self.ISPort)
        ##            sp = subprocess.Popen( com,
        ##                                   shell=True, 
        ##                                   stdout=subprocess.PIPE, 
        ##                                   stderr=subprocess.PIPE )
        ##            
        ##            out,err = sp.communicate()
        ##
        ##            if not sp.returncode:
        ##                self.ISRunning = True
        ##            else:
        ##                self.ISPort += 1
        ##                continue
        ##
        ##        #port is not available,try another port
        ##        except socket.error,msg:
        ##            self.ISPort += 1
        ##            continue
        ##
        ##        try:
        ##            clientSock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        ##            clientSock.connect( (self.hostName, self.ISPort) )
        ##            clientSock.send("getload")
        ##            clientSock.settimeout(1.0)
        ##            self.load = clientSock.recv(64)
        ##            clientSock.close()
        ##
        ##            self.ISRunning = True
        ##            if self.ISPortChanged:
        ##                self.compupterInfo.setISPort(self.ISPort)
        ##            return
        ##        except socket.error:
        ##            self.ISPort += 1
        ##            self.ISPortChanged = True
        ##            continue
        ##
        ##    if cnt==10:
        ##        self.errorOutput += "Could not connect to InfoServer on %s" % self.hostName
        ##elif self.computerInfo and self.checkAll:	# connect to host (if required) and grep loadavg
        ##    try:
        ##        com = 'ssh -x -a %s "cat /proc/loadavg"' % (self.hostName)
        ##        sp = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ##        out,err = sp.communicate()
        ##
        ##        if not sp.returncode:
        ##            self.load = out
        ##            return
        ##        else:
        ##            return
        ##
        ##    except socket.error,msg:
        ##        pass
        ##
        ##
        ##return None

    def getLoad( self ):
        """! @brief get load"""
    
        load = None

        if self.ISPort:
            # try to connect to InfoServer
            clientSock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            try:
                clientSock.connect( (self.host, self.ISPort) )
                clientSock.settimeout(1.0)
                clientSock.send( "getload" )
                load = clientSock.recv(64)
                clientSock.close()

                self.ISRunning = True
            except socket.timeout:
                print "[{t}] ERROR: Timeout on {h}:{p}".format(t=str(datetime.now()),h=self.host, p=self.ISPort )
                return "timeout"
            
        else:
            # check load by log into host and grep /proc/loadavg
            try:
                # create temporary file object for stdout and stderr of executing command
                stdOut = tempfile.TemporaryFile( bufsize=0 )
                stdErr = tempfile.TemporaryFile( bufsize=0 )

                com = 'ssh -x -a -oConnectTimeout=1 -oConnectionAttempts=1 %s "cat /proc/loadavg"' % (self.host)
                sp = subprocess.Popen(com, 
                                      shell=True, 
                                      stdout=stdOut, 
                                      stderr=stdErr)
                sp.wait()
                
                stdOut.seek(0)
                stdErr.seek(0)
                out = stdOut.readlines()
                err = stdErr.read()

                if not sp.returncode and len(out)>0:
                    load = map(float, out[0].strip('\n').split(" ")[:3] )
            except:
                print "ERROR"
                pass

        return load
            
class hProcVersion(Thread):
    def __init__(self,hostName,computerInfo,checkAll=False):
        Thread.__init__(self)
        self.hostName = hostName		# hostName for getting /proc/version
        self.computerInfo = computerInfo	# computerinfo of host
        self.procVersion = None			# /proc/version of host
        self.ISPort = None			# InfoServer port on host
        self.ISPortChanged = False		# set True when port has been changed
        self.ISRunning = False			# running status of InfoServer on host
        self.checkAll = checkAll		# get /proc/version also for host without IS
        self.errorOutput = ""			# output of socket connection errors
        self.errorLog = ""			# log of socket connection errors

    def run(self):
        """ get /proc/version of computer hostName"""

        if self.computerInfo and self.computerInfo.getAllowIS():
            self.ISPort = self.computerInfo.getISPort()	# get stored InfoServer port
            if not self.ISPort: self.ISPort = 21222	# Default port

            # try to connect to InfoServer
            clientSock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            try:
                clientSock.connect( (self.hostName, self.ISPort) )
                clientSock.settimeout(1.0)
                clientSock.send("getprocversion")
                self.procVersion = clientSock.recv(128)
                clientSock.close()

                self.ISRunning = True
                return
            except socket.timeout:
                self.errorOutput += 'Timeout while getting /proc/version of %s:%s\n' % (self.hostName,self.ISPort)
                self.errorLog += "[%s]\n" % strftime("%d %b %Y %H:%M:%S")
                self.errorLog += "Timeout while getting /proc/version of %s:%s" % (self.hostName,self.ISPort)

                return

            except socket.error:
                pass
                #self.port += 1
                #isPort = None

            # ping host
            p = pingHost(self.hostName)
            p.start()
            p.join()

            # check status
            if p.status[1]==0:	# p.status: (transmitted,received)
                # unreachable
                return

            # try to start new InfoServer
            cnt = 0
            while(cnt!=10):
                cnt+=1

                #print "ISPort %s:%s" % (self.hostName,self.ISPort)
                # start InfoServer
                try:
                    com = 'ssh -x -a %s "python %s/InfoServer.py %s"' %(self.hostName, serverpath, self.ISPort)
                    sp = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    out,err = sp.communicate()

                    if not sp.returncode:
                        self.ISRunning = True
                    else:
                        self.ISPort += 1
                        self.ISPortChanged = True
                        continue

                #port is not available,try another port
                except socket.error,msg:
                    self.ISPort += 1
                    self.ISPortChanged = True
                    continue

                try:
                    clientSock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
                    clientSock.connect( (self.hostName, self.ISPort) )
                    clientSock.send("getprocversion")
                    clientSock.settimeout(1.0)
                    self.procVersion = clientSock.recv(128)
                    clientSock.close()

                    self.ISRunning = True
                    if self.ISPortChanged:
                        self.compupterInfo.setISPort(self.ISPort)
                    return
                except socket.error:
                    self.ISPort += 1
                    self.ISPortChanged = True
                    continue

            if cnt==10:
                self.errorOutput += "Could not connect to InfoServer on %s" % self.hostName
        elif self.computerInfo and self.checkAll:	# connect to host (if required) and grep /proc/version
            try:
                com = 'ssh -x -a %s "cat /proc/version"' % (self.hostName)
                sp = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out,err = sp.communicate()

                if not sp.returncode:
                    self.procVersion = out
                    return
                else:
                    return

            except socket.error,msg:
                pass


        return None

def getDefaultTMSPort( user ):
    """! @brief generate default port number for tms of user

    @param user (string) user name
    
    the port number should be larger than 1024 and smaller than 65536
       1. construct list with ascii code of each letter (uses ord() function), e.g., 'dummy' -> [100, 117, 109, 109, 121]
       2. calculate sum: ord(1. letter)*2^0 + ord(2. letter)*2^1 + ...
       3. make sue that sum ist between 10^10 and 10^16
    """
    
    defaultTMSPort = 2**10 + sum( [ x[0]*2**x[1] for x in zip( [ ord(c) for c in user ], range(len(user)) ) ] ) % (2**16-2**10)

    return defaultTMSPort
    
    
def getDefaultTMMSPort( user ):
    """! @brief generate default port number for tmms of user

    @param user (string) user name
    
    the port number should be larger than 1024 and smaller than 65536
       1. construct list with ascii code of each letter (uses ord() function), e.g., 'dummy' -> [100, 117, 109, 109, 121]
       2. calculate sum: ord(1. letter)*2^1 + ord(2. letter)*2^2 + ...
       3. make sue that sum ist between 10^10 and 10^16
    """
    
    defaultTMMSPort = 2**10 + sum( [ x[0]*2**x[1] for x in zip( [ ord(c) for c in user ], range(1,len(user)+1) ) ] ) % (2**16-2**10)

    return defaultTMMSPort
    
    

class TerminateTMMS(Thread):
    def __init__(self,hostName,tmmsPid,TMS):
        Thread.__init__(self)
        self.hostName = hostName		# hostName of TMMS
        self.tmmsPid = tmmsPid			# pid of TMMS

    def run(self):
        """ send kill command to tmms and wait """
        try:
            # kill TMMS
            if TMS.verboseMode:
                sys.stdout.write("  ... %s [%s] send signal SIGINT\n" % (self.hostName,self.tmmsPid))
                sys.stdout.flush()

            sp = subprocess.Popen(['ssh', '-x', '-a', self.hostName, "kill -s INT %s" % self.tmmsPid])
            sp.wait()

            if TMS.verboseMode:
                sys.stdout.write("  ... termination of %s was successful\n" % (self.hostName))
                sys.stdout.flush()

        except:
            try:
                if TMS.verboseMode:
                    sys.stdout.write("  ... %s [%s] send signal SIGINT\n" % (self.hostName,self.tmmsPid))
                    sys.stdout.flush()

                sp = subprocess.Popen(['ssh', '-x', '-a', self.hostName, "kill -s INT %s" % self.tmmsPid])
                sp.wait()
            except:
                # do something else?
                if TMS.verboseMode:
                    print "... killing of TMMS failed!"


class KillJobs(Thread):
    """kill jobs (in thread) with jobIDs by sending request to TMMS and wait"""
    def __init__(self,jobIDs,tmmsHost,tmmsPort,TMS):
        Thread.__init__(self)
        self.jobIDs = jobIDs
        self.tmmsHost = tmmsHost
        self.tmmsPort = tmmsPort
        self.TMS = TMS
        self.ret = ""

    def run(self):
        # loop over all jobIDs and create command for TMMS
        cmd = "killjobs"
        for jobID in self.jobIDs:
            if jobID in self.TMS.jobsDict:
                pid = self.TMS.jobsDict[jobID].getJobInfo('pid')
                cmd += ":%s" % pid

        # connect to TMMS and send kill signal
        tmmsConn = TMConnection(self.tmmsHost,
                                self.tmmsPort,
                                sslConnection=TMS.sslConnection,
                                keyfile=keyfile,
                                certfile=certfile,
                                ca_certs=ca_certs,
                                catchErrors=False,
                                loggerObj=logger)

        if tmmsConn.openConnection:
            tmmsConn.sendAndRecvAndClose(cmd)
            if tmmsConn.requestSent:
                self.ret = tmmsConn.response
            else:
                self.ret = "Killing jobs has failed!"
        else:
            # do something??
            self.ret = "Killing jobs has failed!"



class KillAllJobs(Thread):
    def __init__(self,hostName,tmmsPort,TMS):
        Thread.__init__(self)
        self.hostName = hostName		# hostName of TMMS
        self.tmmsPort = tmmsPort		# port of TMMS

    def run(self):
        """ send kill command to tmms and wait """
        try:
            if TMS.verboseMode:
                sys.stdout.write("  ... %s:%s\n" % (self.hostName,self.tmmsPort))
                sys.stdout.flush()

            tmmsConn = TMConnection(self.hostName,
                                    self.tmmsPort,
                                    sslConnection=TMS.sslConnection,
                                    keyfile=keyfile,
                                    certfile=certfile,
                                    ca_certs=ca_certs,
                                    catchErrors=False,
                                    loggerObj=logger)

            cmd = "killjobs"
            jobList = []
            for jobID,jobInfo in TMS.jobsDict.iteritems():
                jobList.append(jobID)
                pid = jobInfo.getJobInfo('pid')
                if pid:
                    cmd += ":%s" % pid

            if tmmsConn.openConnection:
                tmmsConn.sendAndRecvAndClose(cmd)

                for jobID in jobList:
                    del TMS.jobsDict[jobID]

                sys.stderr.write("  ... killing of all jobs in %s:%s was successful!\n" % (self.hostName,self.tmmsPort) )
            else:
                # do something?
                pass
        except:
            sys.stderr.write("%s" % sys.exc_info()[1])
            sys.stderr.flush()
            # do something?
            if TMS.verboseMode:
                sys.stderr.write("  ... killing of all jobs in %s:%s has failed!\n" % (self.hostName,self.tmmsPort) )


class CleanHost(Thread):
    """ clean up host by deleting all temporary files on host """
    def __init__(self,hostName,TMS):
        Thread.__init__(self)
        self.hostName = hostName		# hostName of TMMS

    def run(self):
        try:
            # kill TMMS
            if TMS.verboseMode:
                sys.stdout.write("  ... %s\n" % (self.hostName))
                sys.stdout.flush()

            sp = subprocess.Popen(['ssh', '-x', '-a', self.hostName, "rm -f /tmp/tmms.*"])
            sp.wait()

            if TMS.verboseMode:
                sys.stdout.write("  ... deleting files on %s was successful\n" % (self.hostName))
                sys.stdout.flush()

        except:
            # do something else?
            if TMS.verboseMode:
                print "... deleting files on %s has failed!" % self.hostName





def cleanUp(TMS):
    """ kill all jobs i every TMMS and terminate each TMMS"""
    # kill all jobs in every TMMS
    hostList = []
    if TMS.verboseMode:
        sys.stdout.write("  kill all jobs in TMMS ...\n")
        sys.stdout.flush()

    for hostName in TMS.cluster:
        tmmsPort = TMS.cluster[hostName].tmmsPort

        current = killAllJobs(hostName,tmmsPort,TMS)
        hostList.append(current)
        current.start()

    # wait until all jobs on every tmms has been killed
    for h in hostList:
        h.join()


    # delete all temporary files on host
    hostList = []
    if TMS.verboseMode:
        sys.stdout.write("  delete all temporary files\n")

    for hostName in TMS.cluster:
        current = CleanHost(hostName,TMS)
        hostList.append(current)
        current.start()

    # wait until all jobs on every tmms have been killed
    for h in hostList:
        h.join()


    # terminate all TMMS
    tmmsList = []
    if TMS.verboseMode:
        sys.stdout.write("  terminate all TMMS ...\n")

    for hostName in TMS.cluster:
        tmmsPid = TMS.cluster[hostName].tmmsPid

        current = TerminateTMMS(hostName,tmmsPid,TMS)
        tmmsList.append(current)
        current.start()

    for t in tmmsList:
         t.join()





        
