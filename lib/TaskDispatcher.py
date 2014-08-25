
import socket
import ssl
import select
import re
import subprocess
import os
import sys
import getopt
from datetime import datetime
from time import time, localtime, strftime, sleep
from random import choice
from string import join,strip
from copy import copy
from threading import Thread
import xml.dom.minidom
import traceback
import json
import ConfigParser
import SocketServer
import threading
import argparse
import textwrap
from pprint import pprint
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import and_, not_, func
from operator import itemgetter, attrgetter

# logging
import logging
logger = logging.getLogger('TaskDispatcher')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)-15s] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.INFO)
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)

# get path to taskmanager. it is assumed that this script is in the bin directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..' ) )

serverpath = '%s/server' % tmpath	# for InfoServer
etcpath    = '%s/etc'    % tmpath	# for configuration files
varpath    = '%s/var'    % tmpath	# for dynamically 
libpath  = '%s/lib' % tmpath		# for hSocket

# include lib path of the TaskManager package to sys.path for loading TaskManager packages
sys.path.insert(0,libpath)

from hSocket import hSocket
from hCommand import hCommand
from hTMUtils import renderHelp, hPingHost, hHostLoad
from hTaskDispatcherInfo import hTaskDispatcherInfo
from hDBConnection import hDBConnection

import hDatabase as db

keyfile = '%s/certs/taskdispatcher.key' % etcpath
certfile = '%s/certs/taskdispatcher.crt' % etcpath
ca_certs = '%s/certs/authorizedKeys.crt' % etcpath


# TaskDispatcher extends the TCPServer
class TaskDispatcher(SocketServer.TCPServer):
    # By setting this we allow the server to re-bind to the address by
    # setting SO_REUSEADDR, meaning you don't have to wait for
    # timeouts when you kill the server and the sockets don't get
    # closed down correctly.
    allow_reuse_address = True

    def __init__(self, port, sslConnection, EOCString, logfileTDErrors, handler, processor, message=''):
        self.host = os.uname()[1]
        self.port = port
        self.sslConnection = sslConnection
        self.EOCString = EOCString
        self.processor = processor
        self.message = message

        SocketServer.TCPServer.__init__(self, (self.host,self.port), handler)

        # write host and port to file <varpath>/taskdispatcher.info
        tdInfo = hTaskDispatcherInfo()
        tdInfo.save([ ('host', self.host),
                      ('port', self.port),
                      ('sslconnection', self.sslConnection),
                      ('eocstring', self.EOCString),
                      ('laststart', str(datetime.now()))
                      ])

        self.dbconnection = hDBConnection()

        #self.testConnection()
        self.fillDatabase()

        # read cluster from config file and update database
        self.initCluster()

        # check if user in etc/users.txt are known
        self.initUsers()

        # save database ids of some entries in self.databaseIDs
        self.databaseIDs = {}
        self.initDatabaseIDs()

        # set interval for loop of calling loop functions
        self.loopInterval = 100

        # set of ids of jobs which are processed currently
        self.lockedJobs = set()
        
        print 
        print "[{t}] TaskDispatcher ({path}) has been started on {h}:{p}\n".format( path=tmpath,
                                                                                    t=str(datetime.now()),
                                                                                    h=self.host,
                                                                                    p=self.port )
        print
                
        self.printStatus()

        
    def run(self):
        """!@brief run server
        
        Start up a server; each time a new request comes in it will be handled by a 
        TaskDispatcherRequestHandler class
        """

        try:
            self.serve_forever()
        except KeyboardInterrupt:
            sys.exit(0)

    def serve_forever(self):
        """!@brief overwrites serve_forever of SocketServer.TCPServer"""

        ## check periodically the database in an own thread
        #l1 = threading.Thread( target=self.loopUpdateHostLoad )
        #l1.setDaemon( True )
        #l1.start()

        ## check periodically the database in an own thread
        l2 = threading.Thread( target=self.loopCheckDatabase )
        l2.setDaemon( True )
        l2.start()
        
        while True:
            self.handle_request()

    def loopUpdateHostLoad( self ):
        """! @brief execute periodically this function """
        
        while True:
            # wait for self.loopInterval seconds
            sleep( self.loopInterval )
            
            # instantiate new socket
            clientSock = hSocket(serverSideSSLConn=False,
                                 sslConnection=self.sslConnection,
                                 EOCString=self.EOCString,
                                 certfile=certfile,
                                 keyfile=keyfile,
                                 ca_certs=ca_certs)

            # connect to server itself
            clientSock.initSocket( self.host, self.port )

            ## send checkdatabase command
            clientSock.send( "updateload" )
            
    def loopCheckDatabase( self ):
        """! @brief execute periodically this function """
        
        while True:
            # instantiate new socket
            clientSock = hSocket(serverSideSSLConn=False,
                                 sslConnection=self.sslConnection,
                                 EOCString=self.EOCString,
                                 certfile=certfile,
                                 keyfile=keyfile,
                                 ca_certs=ca_certs)

            # connect to server itself
            clientSock.initSocket( self.host, self.port )

            ## send checkdatabase command
            clientSock.send( "checkdatabase" )
            
            # wait for self.loopInterval seconds
            sleep( self.loopInterval )
            

    def fillDatabase( self ):
        """! @brief fill database and save ids"""

        logger.info( "Fill database (if necessary)" )
        
        # fill JobStatus table
        js = [ 'waiting', 'pending', 'running', 'finished' ]
        for jobStatus in js:
            try:
                self.dbconnection.query( db.JobStatus ).filter( db.JobStatus.name==jobStatus ).one()
            except:
                logger.info( "  Add entry '{j}' to JobStatus table.".format(t=str(datetime.now()), j=jobStatus ) )
                
                jobStatusInstance = db.JobStatus( name=jobStatus )
                
                self.dbconnection.introduce( jobStatusInstance )
                self.dbconnection.commit()
        logger.info( "... done" )

                
    def initDatabaseIDs( self ):
        """! @brief save some database ids in self.databaseIDs
        """

        self.databaseIDs = dict( self.dbconnection.query( db.JobStatus.name, db.JobStatus.id ).all() )

        
    def initCluster( self ):
        """!@brief read config file and update cluster info in database"""

        # read config file
        try:
            configFileName = '{etcpath}/cluster.cfg'.format(etcpath=etcpath)
            parser = ConfigParser.SafeConfigParser()
            
            if os.path.exists( configFileName ):
                parser.read( configFileName )

                # get cluster name
                clusterName = parser.get( 'CLUSTER', 'name' )
                ##try:
                ##    cluster = self.dbconnection.query( db.Cluster ).filter( db.Cluster.name==clusterName ).one()
                ##except NoResultFound:
                ##    # create cluster in database
                ##    logger.info( "Add Cluster {name}".format(t=str(datetime.now()), name=clusterName ) )
                ##    
                ##    cluster = db.Cluster( name=clusterName )
                ##
                ##    self.dbconnection.introduce( cluster )
                ##    self.dbconnection.commit()
                
                # get all hosts
                hosts = parser.get( 'CLUSTER', 'hosts' ).replace("'",'"')
                hosts = json.loads( hosts )

                # iterate over all hosts and add to database if not present already
                for host in hosts:
                    config = dict( parser.items( host.upper() ) )

                    defaultSettings = { 'full_name': '',
                                        'short_name': '',
                                        'number_slots': 0,
                                        'max_number_occupied_slots': 0,
                                        'additional_info': '',
                                        'allow_info_server': False,
                                        'info_server_port': 0 }
                    
                    defaultStatus = { 'active': False }
                    
                    # get host setting using dict comprehension, prefer settings from config file
                    # automatically cast value to type of value given in defaultSettings
                    hostSettings = { key: type(defaultValue)(config.get(key, defaultValue)) for key,defaultValue in defaultSettings.iteritems() }
                    hostStatus = { key: type(defaultValue)(config.get(key, defaultValue)) for key,defaultValue in defaultStatus.iteritems() }
                    
                    # add entry in database if not already present
                    try:
                        self.dbconnection.query( db.Host ).filter( db.Host.full_name==hostSettings['full_name'] ).one()
                    except NoResultFound:
                        logger.info( "Add Host {name} to cluster {clusterName}".format(t=str(datetime.now()), name=hostSettings['short_name'], clusterName=clusterName ) )

                        # create entry in database
                        host = db.Host( **hostSettings )
                        
                        # create HostSummaryInstance
                        hostSummary = db.HostSummary( host=host,
                                                      available=hostStatus['active'] )
                        
                        self.dbconnection.introduce( host, hostSummary )
                self.dbconnection.commit()

        except:
            traceback.print_exc(file=sys.stdout)
            
        # check reachability of cluster hosts
        self.setReachabilityOfHosts()

        
    def initUsers( self ):
        """! @brief add all user in etc/roles to database"""

        users = {}
        # read users and roles from file
        #    <USER1>   <ROLE1>,<ROLE2>,...
        #    ...

        if os.path.exists('%s/users' % etcpath):
            with open('%s/users' % etcpath) as f:
                users = map(lambda l: l.strip().split('\t'), f.readlines())
                users = { e[0]: set( e[1].split(",") ) for e in users }
        else:
            users = {}

        for user in users:
            try:
                userInstance = self.dbconnection.query( db.User ).filter( db.User.name==user ).one()
            except NoResultFound:
                # add user
                logger.info( "Add User {name}".format(name=user  ) )

                userInstance = db.User( name=user )

            # get already associated roles
            assocRoleNames = set( [r.role.name for r in userInstance.roles ] )
            
            # check roles
            for role in users[ user ]:
                if role not in assocRoleNames:
                    # create new association
                    newAssoc = db.AssociationUserRole()
                    
                    # get role if existing
                    try:
                        roleInstance = self.dbconnection.query( db.Role ).filter( db.Role.name==role ).one()
                    except NoResultFound:
                        # add user
                        logger.info( "Add Role {name}".format(name=role) )

                        roleInstance = db.Role( name=role )

                    # add role to association
                    newAssoc.role = roleInstance

                    # add association to user
                    userInstance.roles.append( newAssoc )
                    
            self.dbconnection.introduce( userInstance )
            self.dbconnection.commit()

        
        
    def setReachabilityOfHosts( self, hosts=[] ):
        """! @brief set reachability of hosts given in list or check all in database 
        
        @param hosts (list) list host full names

        @return (dict) dictinary with hosts and their reachability status
        """

        #dbconnection = hDBConnection( self.dbconnection.ScopedSession )
        dbconnection = hDBConnection()

        if not hosts:
            # get all hosts given in database
            hosts = map(itemgetter(0), dbconnection.query( db.Host.full_name ).all())
        
        logger.info("Checking reachabilty of {n} host{s} ...".format( n=len(hosts), s="s" * int( len(hosts)>1 ) ) )

        pingList = []
        for host in hosts:
            current = hPingHost( host )
            pingList.append( current )
            current.start()

        reachability = {}
        for p in pingList:
             p.join()
             if p.status[1]>0:	# p.status: (transmitted,received)
                 # successful ping
                 logger.info( "     {h} ... is reachable".format( h=p.host ) )

                 # set host in database as reachable
                 hostSummaryInstance = dbconnection.query( db.HostSummary ).join( db.Host ).filter( db.Host.full_name==p.host ).one()
                 hostSummaryInstance.reachable = True

                 reachability[ p.host ] = True
             else:
                 # unreachable
                 logger.info( "     {h} ... is not reachable".format( h=p.host ) )

                 # set host in database as not reachable
                 hostSummaryInstance = dbconnection.query( db.HostSummary ).join( db.Host ).filter( db.Host.full_name==p.host ).one()
                 hostSummaryInstance.reachable = False
                 hostSummaryInstance.active = False
                 
                 reachability[ p.host ] = False
                 
        dbconnection.commit()

        return reachability

        
    def activateHost( self, host ):
        """! @brief set status of host to active if it is available and reachable

        @param host (string) full host name
        """

        #dbconnection = hDBConnection( self.dbconnection.ScopedSession )
        dbconnection = hDBConnection()
        
        reachability = self.setReachabilityOfHosts( hosts=[host] )
        
        if reachability[ host ]:
            # maybe it is more efficient to use query().join().update( ) but in SQLite it is currently not supported (OperationalError)
            hostSummaryInstance = dbconnection.query( db.HostSummary ).join( db.Host ).filter( db.Host.full_name==host ).one()
            hostSummaryInstance.active = True
            
            dbconnection.commit()

        return 

        
    def deactivateHost( self, host ):
        """! @brief set status of host to non-active

        @param host (string) full host name
        """

        dbconnection = hDBConnection()
        
        hostSummaryInstance = dbconnection.query( db.HostSummary ).join( db.Host ).filter( db.Host.full_name==host ).one()
        hostSummaryInstance.active = False
            
        dbconnection.commit()

        return 
        
            
    def updateLoadOfHosts( self, hosts=[] ):
        """! @brief set load of hosts (or of all given in database) by connecting to server and grep /proc/loadavg
        
        @param hosts (list) list Host instances

        @return nothing
        """

        dbconnection = hDBConnection()
        
        if not hosts:
            # get all hosts given in database
            hosts = dbconnection.query( db.Host ).all()

        hostsDict = { h.full_name: h for h in hosts }
        
        logger.info("Checking load of {n} host{s} ...".format( n=len(hosts), s="s" * int( len(hostsDict)>1 ) ) )

        hostLoadList = []
        # iterate over keys
        for host in hostsDict:
            current = hHostLoad( host )
            hostLoadList.append( current )
            current.start()

        for p in hostLoadList:
             p.join()
             logger.info( "     {h} has load {l}".format( h=p.host, l=p.load[0] ) )

             # update load in database
             host = hostsDict[ p.host ]
             
             # get all HostLoad instances attached to Host
             hostLoads = dbconnection.query( db.HostLoad ).join( db.Host ).order_by( db.HostLoad.datetime ).all()
             
             # check whether there is a no newer entry
             if len(hostLoads)==0 or hostLoads[-1].datetime<datetime.now():
                 # store only the last 5 load information
                 if len(hostLoads)>4:
                     # delete oldest one
                     dbconnection.delete( hostLoads[0] )
                     
                 # create new HostLoad instance
                 hostLoad = db.HostLoad( host = host,
                                         loadavg_1min = p.load[0],
                                         loadavg_5min = p.load[1],
                                         loadavg_10min = p.load[2] )

                 dbconnection.introduce( hostLoad )

        dbconnection.commit()

        
    def getNextJob( self, excludedJobIDs=set([]) ):
        """! @brief get next job which will be send to cluster 

        @param excludedJobIDs (set) set of jobIDs which should not be considered

        @todo think about something more sophisticated than just taking the next in queue
        """

        dbconnection = hDBConnection()
        
        # get next waiting job in queue
        if excludedJobIDs:
            #job = self.dbconnection.query( db.Job ).join( db.CurrentJobStatus ).filter( and_( db.CurrentJobStatus.job_status_type_id==self.databaseIDs['waiting'], not_(db.CurrentJobStatus.job_id.in_(excludedJobIDs) ) ) ).order_by( db.CurrentJobStatus.id ).limit(1).all()
            job = dbconnection.query( db.Job ).join( db.JobDetails ).filter( and_( db.JobDetails.job_status_id==self.databaseIDs['waiting'], not_(db.Job.id.in_(excludedJobIDs) ) ) ).order_by( db.Job.id ).first()
            
        else:
            #job = self.dbconnection.query( db.Job ).join( db.CurrentJobStatus ).filter( db.CurrentJobStatus.job_status_type_id==self.databaseIDs['waiting'] ).order_by( db.CurrentJobStatus.id ).limit(1).all()
            job = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==self.databaseIDs['waiting'] ).order_by( db.Job.id ).first()
            
        if job:
            # return job id
            return job.id
        else:
            # no job was found
            None

        
    def getVacantHost( self, slots, excludedHosts=set([]) ):
        """! @brief get vacant host which is not in excludedHosts and has at least slots unused slots

        @param slots (int) minimum number of free slots on vacant host
        @param excludedHosts (set) set of full names of host which should be excluded

        @return hostID or None
        """

        dbconnection = hDBConnection()
        
        logger.info( "find vacant host ..." )
        
        if excludedHosts:
            hosts = dbconnection.query( db.Host ). \
              join( db.HostSummary ). \
              filter( and_(db.HostSummary.available==True,
                           db.HostSummary.reachable==True,
                           db.HostSummary.active==True,
                           not_(db.Host.full_name.in_( excludedHosts ) ),
                           db.Host.max_number_occupied_slots >= db.HostSummary.number_occupied_slots+slots
                           ) ).all()
        else:
            hosts = dbconnection.query( db.Host ). \
              join( db.HostSummary ). \
              filter( and_(db.HostSummary.available==True,
                           db.HostSummary.reachable==True,
                           db.HostSummary.active==True,
                           db.Host.max_number_occupied_slots >= db.HostSummary.number_occupied_slots+slots ) ).all()

        if not hosts:
            logger.info( "... no vacant host found." )
            
            return None
        else:
            # check load

            # pick randomly a host for list
            host = choice( hosts )
            
            logger.info( "... {h} is vacant.".format(h=host.full_name) )

            # get latest load
            try:
                logger.info( "... check load ..." )
                
                hostLoad = sorted( host.host_load, key=attrgetter( 'datetime' ) )[-1]
            except:
                # no load is given
                logger.info( ".... .... no load of host {h} is given.".format(h=host.full_name) )
                return self.getVacantHost( slots, excludedHosts=excludedHosts.add( host.full_name ) )
            
            if hostLoad.loadavg_1min <= host.max_number_occupied_slots:
                logger.info( "... ... load is {l}. ok".format(l=hostLoad.loadavg_1min) )
                return (host.id,host.full_name)
            else:
                # load is too high
                logger.info( "... ... load is {l}. too high".format(l=hostLoad.loadavg_1min) )

                # get another vacant host
                return self.getVacantHost( slots, excludedHosts=excludedHosts.add( host.full_name ) )
                                                                                   
             

        
    def printStatus(self):
        """!@brief print status of server to stdout"""

        dbconnection = hDBConnection()
        
        # get all number of jobs for each status type
        counts = dict( dbconnection.query( db.JobStatus.name, func.count('*') ).join( db.JobDetails, db.JobDetails.job_status_id==db.JobStatus.id ).group_by( db.JobStatus.name ).all() )

        if not counts:
            # no jobs so far in the database
            counts = {}
        
        slotInfo = dbconnection.query( func.count('*'),
                                       func.sum( db.Host.max_number_occupied_slots ), 
                                       func.sum( db.HostSummary.number_occupied_slots ) ).filter( db.HostSummary.active==True ).one()

        if slotInfo[0]==0:
            slotInfo = (0, 0, 0)
            
        print "----------------------------"
        #print "[{t}] Status of TaskDispatcher on {h}:{p}".format(t=str(datetime.now()), h=self.host, p=self.port)
        logger.info( "Status of TaskDispatcher on {h}:{p}".format(t=str(datetime.now()), h=self.host, p=self.port) )
        print
        print "{s:>20} : {value}".format(s="active hosts", value=slotInfo[0] )
        print "{s:>20} : {value}".format(s="occupied slots", value="{occupied} / {total}".format(occupied=slotInfo[2],total=slotInfo[1]) )
        print "{s:>20} : {value}".format(s="waiting jobs", value=counts.get('waiting',0) )
        print "{s:>20} : {value}".format(s="pending jobs", value=counts.get('pending',0) )
        print "{s:>20} : {value}".format(s="running jobs", value=counts.get('running',0) )
        print "{s:>20} : {value}".format(s="finished jobs", value=counts.get('finished',0) )
        print "----------------------------"

        
# The RequestHandler handles an incoming request.
class TaskDispatcherRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, clientAddress, TD):
        self.TD = TD
        SocketServer.BaseRequestHandler.__init__(self, request, clientAddress, self.TD)

    def handle(self):
        (sread, swrite, sexc) = select.select([self.request], [], [], None)

        # create a hSocket-instance
        requestSocket = hSocket( sock=self.request, 
                                 serverSideSSLConn=True,
                                 catchErrors=False, 
                                 EOCString=self.TD.EOCString )
        receivedStr = strip(requestSocket.recv())
        
        logger.info( "NEW REQUEST: {r}".format(t=str(datetime.now()),r=receivedStr ) )

        t1 = datetime.now()
        
        # process request
        try:
            self.TD.processor.process(receivedStr, requestSocket, self.TD)
        except:
            # processing failed
            tb = sys.exc_info()

            logger.error( ' [{h}:{p}] Error while processing request!\n'.format(h=requestSocket.host,p=requestSocket.port))

            traceback.print_exception(*tb,file=sys.stderr)
            sys.stderr.flush()

            requestSocket.send("Error while processing request!\n%s" %  tb[1])

        t2 = datetime.now()
        logger.info( "... done in {dt}s.".format(dt=str(t2-t1) ) )

        
    def finish(self):
        """! @brief Send jobs to vacant hosts and cleanup handler afterwards

        Check database if there are waiting jobs, check user, find vacant host, and send job to associated tms of user.
        """
        
        dbconnection = hDBConnection()
        
        print "----------------------------"

        t1 = datetime.now()
        
        logger.info( "get next job to be executed ..." )
                     
        # get next job to be ready to be sent to associate tms
        jobID = self.TD.getNextJob( excludedJobIDs=self.TD.lockedJobs )

        if jobID:
            # add job to locked jobs, i.e., they are currently processed
            self.TD.lockedJobs.add( jobID )
            
            # get Job instance
            job = dbconnection.query( db.Job ).get( jobID )
            user = job.user

            logger.info( "... found job {i} of user {u}".format( i=jobID, u=user.name ) )
            
            # get excluded hosts
            excludedHosts = json.loads( job.excluded_hosts )
            
            # get vacant host which has the necessary number of free slots
            vacantHost = self.TD.getVacantHost( job.slots, excludedHosts=set( excludedHosts) )

            if vacantHost:
                logger.info( "run job {j} of user {u} on {h}".format(j=jobID, u=user.name, h=vacantHost[1] ) )

                try:
                    # send jobID to tms
                    sock = hSocket(host=user.tms_host,
                                   port=user.tms_port,
                                   EOCString=self.TD.EOCString,
                                   sslConnection=self.TD.sslConnection,
                                   certfile=certfile,
                                   keyfile=keyfile,
                                   ca_certs=ca_certs,
                                   catchErrors=False)

                    if sock:
                        # set job as pending
                        dbconnection.query( db.JobDetails.job_id ).\
                          filter( db.JobDetails.job_id==jobID ).\
                          update( {db.JobDetails.job_status_id: self.TD.databaseIDs['pending']} )


                        # reduce slots in host
                        dbconnection.query( db.HostSummary ).\
                          filter( db.HostSummary.host_id==vacantHost[0] ).\
                          update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots+job.slots })

                        # set history
                        jobHistory = db.JobHistory( job=job,
                                                    job_status_id = self.TD.databaseIDs['pending'] )
                        
                        dbconnection.introduce( jobHistory )
                        
                        dbconnection.commit()

                        # set job to tms
                        jsonObj = json.dumps( { 'jobID': jobID,
                                                'hostID': vacantHost[0],
                                                'hostName': vacantHost[1] })
                        sock.send( 'runjob:{j}'.format( j=jsonObj ) )

                except socket.error,msg:
                    logger.info( "... failed [{h}:{p}]: {m}".format(h=user.tms_host, p=user.tms_port, m=msg) )

            # remove job from locked ids
            self.TD.lockedJobs.remove( jobID )

                    
                
        else:
            logger.info( '... no job.' )

        t2 = datetime.now()
        logger.info( "... done in {dt}s.".format(dt=str(t2-t1) ) )
        
        self.TD.printStatus()
        
        return SocketServer.BaseRequestHandler.finish(self)


        
# This class is used to process the commands
class TaskDispatcherRequestProcessor(object):
    def __init__( self ):
        ############
        # define commands
        ############
        # specific help could be specified here as well
        self.commands = {}	# {<COMMAND>: TDCommand, ...}

        
        self.commands["PING"] = hCommand( command_name = "ping",
                                          regExp = '^ping$',
                                          help = "return a pong")
        self.commands["HELP"] = hCommand( command_name = "help",
                                          regExp = "^help$",
                                          help = "return help")
        self.commands["FULLHELP"] = hCommand( command_name = "fullhelp",
                                              regExp = "^fullhelp$",
                                              help = "return full help")
        self.commands["UPDATELOAD"] = hCommand( command_name = "updateload",
                                              regExp = "^updateload$",
                                              help = "update load of hosts")
        self.commands["CHECKDATABASE"] = hCommand( command_name = "checkdatabase",
                                                   regExp = "^checkdatabase$",
                                                   help = "check database for waiting jobs")
        self.commands["SETINTERVAL"] = hCommand( command_name = "setinterval",
                                                 arguments = "<TIME>",
                                                 regExp = "^setinterval:(.*)",
                                                 help = "set interval in seconds for loop checking the database")
        self.commands["ADDJOB"] = hCommand(command_name = 'addjobJSON',
                                           arguments = "<jsonStr>",
                                           regExp = '^addjob:(.*)',
                                           help = "add job from user to database")
        self.commands["GETTDSTATUS"] = hCommand( command_name = "gettdstatus",
                                                 regExp = "^gettdstatus$",
                                                 help = "return info about status of the TaskDispatcher")
        self.commands["LSWJOBS"] = hCommand(command_name = 'lswjobs',
                                           regExp = '^lswjobs$',
                                           help = "return waiting jobs")
        
        ##self.commands["LSPJOBS"] = hCommand(command_name = 'lspjobs',
        ##                                   regExp = '^lspjobs$',
        ##                                   help = "return pending jobs")
        
        self.commands["LSRJOBS"] = hCommand(command_name = 'lsrjobs',
                                           regExp = '^lsrjobs$',
                                           help = "return running jobs")
        
        self.commands["LSFJOBS"] = hCommand(command_name = 'lsfjobs',
                                           regExp = 'lsfjobs',
                                           help = "return finished jobs")

        
        self.commands["ACTIVATEHOST"] = hCommand(command_name = 'activatehost',
                                                 arguments = "<host>",
                                                 regExp = '^activatehost:(.*)',
                                                 help = "activate host, i.e., set status to active.")
        
        self.commands["DEACTIVATEHOST"] = hCommand(command_name = 'deactivatehost',
                                                 arguments = "<host>",
                                                 regExp = '^deactivatehost:(.*)',
                                                 help = "deactivate host, i.e., set status to non-active.")

        #self.commands["REGISTERUSER"] = hCommand(command_name = 'registeruser',
        #                                         arguments = "<username>",
        #                                         regExp = '^registeruser:(.*)',
        #                                         help = "register user to taskmanager.")
        
        self.commands["PROCESSFINISHED"] = hCommand(command_name = 'ProcessFinished',
                                                    arguments = "<jobID>",
                                                    regExp = '^ProcessFinished:(.*)',
                                                    help = "Info (from TMS) about a finished job.")


        

        
    def process(self, requestStr, request, TD):
        """Process a requestStr"""

        dbconnection = hDBConnection()
        
        #####################
        # check if request is a known command

        if self.commands["PING"].re.match(requestStr):
            request.send("pong")
            
        elif self.commands["FULLHELP"].re.match(requestStr):
            help = []
            help.append( "Commands known by the TaskDispatcher:" )
            # iterate over all commands defined in self.commands
            help.extend( renderHelp( sorted(self.commands.keys()), self.commands ) )
            request.send( '\n'.join( help ) )
            
        elif self.commands["UPDATELOAD"].re.match( requestStr ):
            # update load of hosts
            TD.updateLoadOfHosts()

            request.send( 'done.' )
        
        elif self.commands["CHECKDATABASE"].re.match( requestStr ):
            # update load of hosts
            TD.updateLoadOfHosts()

            ### get all waiting jobs
            ##jobIDs = TD.dbconnection.query( db.CurrentJobStatus.job_id ).filter( db.CurrentJobStatus.job_status_type_id==TD.databaseIDs['added'] ).all()
            ##
            ##print "number of waiting jobs:",len(jobIDs)
            
        elif self.commands['SETINTERVAL'].re.match( requestStr ):
            c = self.commands["SETINTERVAL"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopInterval = interval

            request.send('done.')
            
        elif self.commands['ACTIVATEHOST'].re.match( requestStr ):
            c = self.commands['ACTIVATEHOST']

            host = c.re.match( requestStr ).groups()[0]

            TD.activateHost( host )

            
        #elif self.commands['REGISTERUSER'].re.match( requestStr ):
        #    c = self.commands['REGISTERUSER']
        #
        #    user = c.re.match( requestStr ).groups()[0]
        #
        #    userInstance = dbconnection.query( db.User ).filter( db.User.name==user ).all()
        #
        #    if len(userInstance)>1:
        #        raise MultipleResultsFound
        #    elif len(userInstance)==0:
        #        # !!! think about a better and more secure way to integrate users
        #        
        #        # create user
        #        newUser = db.User( name=user )
        #        dbconnection.introduce( newUser )
        #        dbconnection.commit()
        #
        #        logger.info( 'Added new user {u}'.format(u=user) )

        elif self.commands['DEACTIVATEHOST'].re.match( requestStr ):
            c = self.commands['DEACTIVATEHOST']

            host = c.re.match( requestStr ).groups()[0]

            TD.deactivateHost( host )
            
        elif self.commands["ADDJOB"].re.match( requestStr ):
            c = self.commands["ADDJOB"]

            jsonObj = c.re.match( requestStr ).groups()[0]
            jsonObj = json.loads( jsonObj )

            command = jsonObj['command']
            slots = jsonObj.get('slots', 1)
            infoText = jsonObj.get('infoText','')
            group = jsonObj.get('group','')
            stdout = jsonObj.get('stdout','')
            stderr = jsonObj.get('stdin','')
            logfile = jsonObj.get('logfile','')
            shell = jsonObj['shell']
            priority = jsonObj.get('priority',1)
            excludedHosts = jsonObj.get("excludedHosts",[])
            user = jsonObj['user']
            tmsHost = jsonObj['TMSHost']
            tmsPort = jsonObj['TMSPort']
            tmsID = jsonObj['TMSID']

            # get user from database
            # !!! think about a better and more secure way to integrate users
            try:
                userInstance = dbconnection.query( db.User ).filter( db.User.name==user ).all()

                if len(userInstance)>1:
                    raise MultipleResultsFound
                elif len(userInstance)==1:
                    userInstance = userInstance[0]
                    
                    # update tms info at user
                    userInstance.tms_host=tmsHost
                    userInstance.tms_port=tmsPort
                    userInstance.tms_id=tmsID
                
                    #dbconnection.introduce( userInstance )
                    dbconnection.commit()
                    
                    user_id = userInstance.id
                else:
                    logger.info( 'Unknown user {u}'.format(u=user) )
                    request.send( 'Unknown user' )
                    
            except:
                traceback.print_exc(file=sys.stderr)

            # create database entry for the new job
            newJob = db.Job( user_id=user_id,
                             command=command,
                             slots=int(slots),
                             info_text=infoText,
                             shell=shell,
                             stdout=stdout,
                             stderr=stderr,
                             logfile=logfile )

            # set jobstatus for this job
            jobDetails = db.JobDetails( job=newJob,
                                        job_status_id=TD.databaseIDs['waiting'] )

            # set history
            jobHistory = db.JobHistory( job=newJob,
                                        job_status_id = TD.databaseIDs['waiting'] )
            
            dbconnection.introduce( newJob, jobDetails, jobHistory )
            
            dbconnection.commit()

            logger.info( 'Added new job with id {i}'.format( i=newJob.id ) )
            
            request.send( str(newJob.id) )
            
            
        elif self.commands["GETTDSTATUS"].re.match( requestStr ):

            request.send( 'some info about this td ...' )

        elif self.commands["LSWJOBS"].re.match( requestStr ):
            jobs = dbconnection.query( db.Job ).join( db.Details ).filter( db.Details.job_status_id==TD.databaseIDs['waiting']).all()
            for job in jobs:
                print job

            request.send( "{n} jobs".format(n=len(jobs)) )
                
        elif self.commands["PROCESSFINISHED"].re.match( requestStr ):
            c = self.commands["PROCESSFINISHED"]

            jobID = c.re.match( requestStr ).groups()[0]
            job = dbconnection.query( db.Job ).get( jobID )

            # free occupied slots from host
            dbconnection.query( db.HostSummary ).\
              filter( db.HostSummary.host_id==job.job_details.host_id ).\
              update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - job.slots } )

            dbconnection.commit()
            
        else:
            request.send("What do you want?")

        return

