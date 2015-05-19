
import socket
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
from collections import defaultdict
from threading import Thread, Lock
import xml.dom.minidom
import traceback
import json
import ConfigParser
import SocketServer
import threading
import argparse
import textwrap
from pprint import pprint as pp
from sqlalchemy import and_, not_, func
from operator import itemgetter, attrgetter
from sqlalchemy.orm.exc import NoResultFound

from hTimeLogger import hTimeLogger
from hJobScheduler import hJobSchedulerPrioritized as JobScheduler
from hLog import hLog

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

# wrap logger
loggerWrapper = hLog( logger )

# TaskDispatcher extends the TCPServer
class TaskDispatcher(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # By setting this we allow the server to re-bind to the address by
    # setting SO_REUSEADDR, meaning you don't have to wait for
    # timeouts when you kill the server and the sockets don't get
    # closed down correctly.
    allow_reuse_address = True
    
    request_queue_size = 50

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

        dbconnection = hDBConnection()
        
        # set last job status update to current time if does not exist
        try:
            dbconnection.query( db.TaskDispatcherDetails.last_job_status_update ).one()
        except NoResultFound:
            taskDispatcherDetails = db.TaskDispatcherDetails( last_job_status_update=datetime.now() )
            dbconnection.introduce( taskDispatcherDetails )
            dbconnection.commit()
        
        self.fillDatabase()

        # read cluster from config file and update database
        self.initClusterByTableFile()
        #self.initClusterByConfigFile()

        # check if user in etc/users.txt are known
        self.initUsers()

        # save database ids of some entries in self.databaseIDs
        self.databaseIDs = {}
        self.initDatabaseIDs()

        self.jobScheduler = JobScheduler()

        # set interval for loop of calling loop functions
        self.loopCheckTDInterval = 1
        self.loopPrintStatusInterval = 5
        self.loopCheckFinishedJobsInterval = 1
        self.loopUpdateHostLoadInterval = 5

        # set of ids of jobs which are processed currently
        #self.lockedJobs = set()

        # flags which indicate a running processing
        self.sendingJobs = False
        self.checkingFinishedJobs = False
        self.printingStatus = False

        print 
        print "[{t}] TaskDispatcher ({path}) has been started on {h}:{p}\n".format( path=tmpath,
                                                                                    t=str(datetime.now()),
                                                                                    h=self.host,
                                                                                    p=self.port )
        print

        # activity status of cluster
        self.active = False

        # a lock
        self.Lock = threading.Lock()
        
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

        ## periodically ping td in an own thread
        l0 = threading.Thread( target=self.loopCheckTD )
        l0.setDaemon( True )
        l0.setName( "(loop) loopCheckTD" )
        l0.date = datetime.now()
        l0.description = "periodically pinging taskdispatcher"
        l0.start()
        
        ## print periodically status of taskmanager in an own thread
        l1 = threading.Thread( target=self.loopPrintStatus )
        l1.setDaemon( True )
        l1.setName( "(loop) loopPrintStatus" )
        l1.date = datetime.now()
        l1.description = "periodically printing status of taskdispatcher"
        l1.start()

        ## update periodically load of hosts
        l2 = threading.Thread( target=self.loopUpdateHostLoad )
        l2.setDaemon( True )
        l2.setName( "(loop) loopUpdateHostLoad" )
        l2.date = datetime.now()
        l2.description = "periodically updating load of hosts"
        l2.start()
        
        ## update periodically check finished jobs
        l3 = threading.Thread( target=self.loopCheckFinishedJobs )
        l3.setDaemon( True )
        l3.setName( "(loop) loopCheckFinishedJobs" )
        l3.date = datetime.now()
        l3.description = "periodically checking finished jobs"
        
        l3.start()
        
        while True:
            self.handle_request()

    def loopCheckTD( self ):
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
            clientSock.send( "check" )
            clientSock.recv()
            
            # wait a little bit
            sleep( self.loopCheckTDInterval )
            
    def loopPrintStatus( self ):
        """! @brief execute periodically this function """
    
        while True:
            self.printStatus()

            # wait a little bit
            sleep( self.loopPrintStatusInterval )


    def loopCheckFinishedJobs( self ):
        """! @brief execute periodically this function """
        
        while True:
            self.checkFinishedJobs()

            # wait a little bit
            sleep( self.loopCheckFinishedJobsInterval )

            
    def loopUpdateHostLoad( self ):
        """! @brief execute periodically this function """
        
        while True:
            self.updateLoadOfHosts()

            # wait a little bit
            sleep( self.loopUpdateHostLoadInterval )
            
            
    ##def loopCheckDatabase( self ):
    ##    """! @brief execute periodically this function """
    ##    
    ##    while True:
    ##        # instantiate new socket
    ##        clientSock = hSocket(serverSideSSLConn=False,
    ##                             sslConnection=self.sslConnection,
    ##                             EOCString=self.EOCString,
    ##                             certfile=certfile,
    ##                             keyfile=keyfile,
    ##                             ca_certs=ca_certs)
    ##
    ##        # connect to server itself
    ##        clientSock.initSocket( self.host, self.port )
    ##
    ##        ## send checkdatabase command
    ##        clientSock.send( "checkdatabase" )
    ##        
    ##        # wait a little bit
    ##        sleep( self.loopCheckDatabaseInterval )
            

    def fillDatabase( self ):
        """! @brief fill database and save ids"""

        loggerWrapper.write( "Fill database (if necessary)" )

        dbconnection = hDBConnection()
        
        # fill JobStatus table
        js = [ 'waiting', 'pending', 'running', 'finished' ]
        for jobStatus in js:
            try:
                dbconnection.query( db.JobStatus ).filter( db.JobStatus.name==jobStatus ).one()
            except NoResultFound:
                loggerWrapper.write( "  Add entry '{j}' to JobStatus table.".format(t=str(datetime.now()), j=jobStatus ) )
                
                jobStatusInstance = db.JobStatus( name=jobStatus )
                
                dbconnection.introduce( jobStatusInstance )
                dbconnection.commit()
        loggerWrapper.write( "... done" )

                
    def initDatabaseIDs( self ):
        """! @brief save some database ids in self.databaseIDs
        """

        dbconnection = hDBConnection()
        
        self.databaseIDs = dict( dbconnection.query( db.JobStatus.name, db.JobStatus.id ).all() )

        
    def initClusterByConfigFile( self ):
        """!@brief read config file and update cluster info in database"""

        dbconnection = hDBConnection()

        # read config file
        try:
            configFileName = '{etcpath}/cluster.cfg'.format(etcpath=etcpath)
            parser = ConfigParser.SafeConfigParser()
            
            if os.path.exists( configFileName ):
                parser.read( configFileName )

                # get cluster name
                clusterName = parser.get( 'CLUSTER', 'name' )
                
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
                        dbconnection.query( db.Host ).filter( db.Host.full_name==hostSettings['full_name'] ).one()
                    except NoResultFound:
                        loggerWrapper.write( "Add Host {name} to cluster {clusterName}".format(t=str(datetime.now()), name=hostSettings['short_name'], clusterName=clusterName ) )

                        # create entry in database
                        host = db.Host( **hostSettings )
                        
                        # create HostSummaryInstance
                        hostSummary = db.HostSummary( host=host,
                                                      available=hostStatus['active'] )
                        
                        dbconnection.introduce( host, hostSummary )
                dbconnection.commit()

        except:
            traceback.print_exc(file=sys.stdout)
            
        # check reachability of cluster hosts
        self.setReachabilityOfHosts()


    def initClusterByTableFile( self ):
        """!@brief read table file and update cluster info in database"""

        dbconnection = hDBConnection()

        # read cluster table
        try:
            tableFileName = '{etcpath}/cluster.tab'.format(etcpath=etcpath)
            
            if os.path.exists( tableFileName ):
                with open( tableFileName ) as f:
                    # skip first line
                    f.readline()

                    # iterate over all lines
                    for line in f:
                        line = line.strip("\n")
                        lineSplitted = line.split("\t")

                        if len(line)>0 and len(lineSplitted)==8:
                            defaultSettings = { 'full_name': '',
                                                'short_name': '',
                                                'number_slots': 0,
                                                'max_number_occupied_slots': 0,
                                                'additional_info': '',
                                                'allow_info_server': False,
                                                'info_server_port': 0 }
                            defaultStatus = { 'active': False }

                            defaultStatus = { 'active': False }
                            
                            # columns: short_name, full_name, number_slots, max_number_occupied_slots, additional_info, allow_info_server, info_server_port, active
                            settings = dict( filter( lambda s: s[1]!="", zip( ["short_name", "full_name", "number_slots", "max_number_occupied_slots", "additional_info", "allow_info_server", "info_server_port", "active"], lineSplitted ) ) )

                            # get host setting using dict comprehension, prefer settings from settings file
                            # automatically cast value to type of value given in defaultSettings
                            hostSettings = { key: type(defaultValue)(settings.get(key, defaultValue)) for key,defaultValue in defaultSettings.iteritems() }
                            hostStatus = { key: type(defaultValue)(settings.get(key, defaultValue)) for key,defaultValue in defaultStatus.iteritems() }

                            # add entry in database if not already present
                            try:
                                dbconnection.query( db.Host ).filter( db.Host.full_name==hostSettings['full_name'] ).one()
                            except NoResultFound:
                                loggerWrapper.write( "Add Host {name} to cluster".format(t=str(datetime.now()), name=hostSettings['short_name'] ) )

                                # create entry in database
                                host = db.Host( **hostSettings )

                                # create HostSummaryInstance
                                hostSummary = db.HostSummary( host=host,
                                                              available=hostStatus['active'] )

                                dbconnection.introduce( host, hostSummary )
                    dbconnection.commit()

        except:
            traceback.print_exc(file=sys.stdout)
            
        # check reachability of cluster hosts
        self.setReachabilityOfHosts()
        
    def initUsers( self ):
        """! @brief add all user in etc/roles to database"""

        dbconnection = hDBConnection()
        
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
                userInstance = dbconnection.query( db.User ).filter( db.User.name==user ).one()
            except NoResultFound:
                # add user
                loggerWrapper.write( "Add User {name}".format(name=user  ) )

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
                        roleInstance = dbconnection.query( db.Role ).filter( db.Role.name==role ).one()
                    except NoResultFound:
                        # add user
                        loggerWrapper.write( "Add Role {name}".format(name=role) )

                        roleInstance = db.Role( name=role )

                    # add role to association
                    newAssoc.role = roleInstance

                    # add association to user
                    userInstance.roles.append( newAssoc )
                    
            dbconnection.introduce( userInstance )
            dbconnection.commit()

        
        
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
        
        loggerWrapper.write("Checking reachabilty of {n} host{s} ...".format( n=len(hosts), s="s" * int( len(hosts)>1 ) ) )

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
                 loggerWrapper.write( "     {h} ... is reachable".format( h=p.host ) )

                 # set host in database as reachable
                 hostSummaryInstance = dbconnection.query( db.HostSummary ).join( db.Host ).filter( db.Host.full_name==p.host ).one()
                 hostSummaryInstance.reachable = True

                 reachability[ p.host ] = True
             else:
                 # unreachable
                 loggerWrapper.write( "     {h} ... is not reachable".format( h=p.host ) )

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
            hosts = dbconnection.query( db.Host ).\
                    join( db.HostSummary ). \
                    filter( and_(db.HostSummary.available==True,
                                 db.HostSummary.reachable==True ) ).all()
                                 

        hostsDict = { h.full_name: h for h in hosts }
        
        loggerWrapper.write("Checking load of {n} host{s} ...".format( n=len(hosts), s="s" * int( len(hostsDict)>1 ) ), logCategory='load' )

        hostLoadList = []
        # iterate over keys
        for host in hostsDict:
            current = hHostLoad( host )
            hostLoadList.append( current )
            current.start()

        for p in hostLoadList:
             p.join()
             loggerWrapper.write( "     {h} has load {l}".format( h=p.host, l=p.load[0] ), logCategory='load' )

             # update load in database
             host = hostsDict[ p.host ]

             # get all HostLoad instances attached to Host
             hostLoads = dbconnection.query( db.HostLoad ).join( db.Host ).filter( db.Host.id==host.id ).order_by( db.HostLoad.datetime ).all()

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

        
    ##def getNextJob( self, excludedJobIDs=set([]) ):
    ##    """! @brief get next job which will be send to cluster 
    ##
    ##    @param excludedJobIDs (set) set of jobIDs which should not be considered
    ##
    ##    @todo think about something more sophisticated than just taking the next in queue
    ##    """
    ##
    ##    dbconnection = hDBConnection()
    ##
    ##    # get next waiting job in queue
    ##    if excludedJobIDs:
    ##        job = dbconnection.query( db.WaitingJob ).\
    ##              join( db.Job ).\
    ##              join( db.User ).\
    ##              filter( db.User.enabled==True ).\
    ##              join( db.JobDetails ).\
    ##              filter( not_(db.Job.id.in_(excludedJobIDs) ) ).\
    ##              order_by( db.Job.id ).first()
    ##        ##job = dbconnection.query( db.Job ).\
    ##        ##      join( db.JobDetails ).\
    ##        ##      filter( and_( db.JobDetails.job_status_id==self.databaseIDs['waiting'], not_(db.Job.id.in_(excludedJobIDs) ) ) ).\
    ##        ##      order_by( db.Job.id ).first()
    ##    else:
    ##        job = dbconnection.query( db.WaitingJob ).\
    ##              join( db.Job ).\
    ##              join( db.User ).\
    ##              filter( db.User.enabled==True ).\
    ##              join( db.JobDetails ).\
    ##              order_by( db.Job.id ).first()
    ##        ##job = dbconnection.query( db.Job ).\
    ##        ##      join( db.JobDetails ).\
    ##        ##      filter( db.JobDetails.job_status_id==self.databaseIDs['waiting'] ).\
    ##        ##      order_by( db.Job.id ).first()
    ##        
    ##    if job:
    ##        # return job id
    ##        return job.job_id
    ##    else:
    ##        # no job was found
    ##        None


    ##def getNextJobs( self, numJobs=1, excludedJobIDs=set([]), returnInstances=False ):
    ##    """! @brief get next jobs which will be send to cluster 
    ##
    ##    @param numJobs (int) maximal number of jobs which will be returned
    ##    @param excludedJobIDs (set) set of jobIDs which should not be considered
    ##    @param returnInstances (bool) if True return db.Job instances otherwise return job ids
    ##
    ##    @return (list) job ids or db.Job
    ##    
    ##    @todo think about something more sophisticated than just taking the next in queue
    ##    """
    ##    #timeLogger = TimeLogger( prefix="getNextJobs" )
    ##    
    ##    dbconnection = hDBConnection()
    ##
    ##    #timeLogger.log( "number excluded jobs: {n}".format(n=len(excludedJobIDs)) )
    ##    # get next waiting job in queue
    ##    #timeLogger.log( "get jobs ..." )
    ##    if excludedJobIDs:
    ##        jobs = dbconnection.query( db.WaitingJob ).\
    ##               join( db.User ).\
    ##               join( db.Job ).\
    ##               filter( db.User.enabled==True ).\
    ##               filter( not_(db.Job.id.in_(excludedJobIDs) ) ).\
    ##               limit( numJobs ).all()
    ##        
    ##        ##jobs = dbconnection.query( db.WaitingJob ).\
    ##        ##       join( db.Job ).\
    ##        ##       join( db.User ).\
    ##        ##       filter( db.User.enabled==True ).\
    ##        ##       filter( not_(db.Job.id.in_(excludedJobIDs) ) ).\
    ##        ##       limit( numJobs ).all()
    ##        
    ##        ##jobs = dbconnection.query( db.Job ).\
    ##        ##       join( db.User ).\
    ##        ##       filter( db.User.enabled==True ).\
    ##        ##       join( db.JobDetails ).\
    ##        ##       filter( and_( db.JobDetails.job_status_id==self.databaseIDs['waiting'], not_(db.Job.id.in_(excludedJobIDs) ) ) ).\
    ##        ##       order_by( db.Job.id ).\
    ##        ##       limit( numJobs ).all()
    ##    else:
    ##        jobs = dbconnection.query( db.WaitingJob ).\
    ##               join( db.User ).\
    ##               filter( db.User.enabled==True ).\
    ##               limit( numJobs ).all()
    ##        
    ##        ##jobs = dbconnection.query( db.WaitingJob ).\
    ##        ##       join( db.Job ).\
    ##        ##       join( db.User ).\
    ##        ##       filter( db.User.enabled==True ).\
    ##        ##       limit( numJobs ).all()
    ##        
    ##        ##jobs = dbconnection.query( db.Job ).\
    ##        ##       join( db.User ).\
    ##        ##       filter( db.User.enabled==True ).\
    ##        ##       join( db.JobDetails ).\
    ##        ##       filter( db.JobDetails.job_status_id==self.databaseIDs['waiting'] ).\
    ##        ##       order_by( db.Job.id ).\
    ##        ##       limit( numJobs ).all()
    ##        
    ##    #timeLogger.log( "number of found jobs: {n}".format(n=len(jobs)) )
    ##    
    ##    if jobs:
    ##        # return job id
    ##        if returnInstances:
    ##            return jobs
    ##        else:
    ##            return [ j.job_id for j in jobs ]
    ##    else:
    ##        # no job was found
    ##        return []

        
    def getVacantHost( self, slots, excludedHosts=set([]) ):
        """! @brief get vacant host which is not in excludedHosts and has at least slots unused slots

        @param slots (int) minimum number of free slots on vacant host
        @param excludedHosts (set) set of full names of host which should be excluded

        @return (hostID,hostName)
        """

        #timeLogger = TimeLogger( prefix="getVacantHost" )
        
        dbconnection = hDBConnection()
        
        loggerWrapper.write( "    find vacant host ..." )
        
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
            loggerWrapper.write( "    ... no vacant host found." )
            
            return None
        else:
            # check load

            # pick randomly a host from list
            host = choice( hosts )
            
            # get latest load
            try:
                hostLoad = sorted( host.host_load, key=attrgetter( 'datetime' ) )[-1]
            except:
                # no load is given
                loggerWrapper.write( "    ... host {h} has no load in db. skip.".format(h=host.full_name) )
                
                # get another vacant host
                excludedHosts.add( host.full_name )
                return self.getVacantHost( slots, excludedHosts=excludedHosts )
            
            if hostLoad.loadavg_1min <= host.max_number_occupied_slots:
                loggerWrapper.write( "  ... {h} is vacant. load is {l}. ok.".format(h=host.full_name,l=hostLoad.loadavg_1min) )
                return (host.id,host.full_name)
            else:
                # load is too high
                loggerWrapper.write( "    ... {h} is vacant. load is {l}. too high. skip".format(h=host.full_name,l=hostLoad.loadavg_1min) )

                # get another vacant host
                excludedHosts.add( host.full_name )
                return self.getVacantHost( slots, excludedHosts=excludedHosts )

                                                                                   
    def checkFinishedJobs(self, dbconnection=None):
        """! @brief check jobs whether they have finished and free occupied slots of hosts
        """

        if not self.checkingFinishedJobs:
            self.checkingFinishedJobs = True
            #timeLogger = TimeLogger( prefix="updateJobStatus" )

            # aquire lock
            #loggerWrapper.write( "  acquire lock ...")
            #self.Lock.acquire()

            # connect to database if not given
            if not dbconnection:
                dbconnection = hDBConnection()

            taskDispatcherDetails = dbconnection.query( db.TaskDispatcherDetails ).one()
            #timeLogger.log( "get finished and unchecked jobs" )
            #jobHistories = dbconnection.query( db.JobHistory ).filter( and_(db.JobHistory.job_status_id == self.databaseIDs['finished'],
            #                                                                db.JobHistory.checked == False) ).all()
            #timeLogger.log( "found {n}".format(n=len(jobHistories) ) )

            #timeLogger.log( "get finished jobs ..." )
            finishedJobs = dbconnection.query( db.FinishedJob ).all()
            #timeLogger.log( "... found {n}".format(n=len(finishedJobs) ) )

            if finishedJobs:
                loggerWrapper.write( "{n} finished job{s}.".format(n=len(finishedJobs), s='s' if len(finishedJobs)>1 else '' ), logCategory='checkingJobs' )

            taskDispatcherDetails.last_job_status_update = datetime.now()

            occupiedSlots = defaultdict( int )
            #for jobHistory in jobHistories:
            #    if jobHistory.job_status_id == self.databaseIDs['finished']:
            #        jobID = jobHistory.job_id
            #        loggerWrapper.write( "   Job [{j}] has finished. Free occupied slots on host.".format(j=jobID ) )
            #
            #        # get Job instance
            #        job = dbconnection.query( db.Job ).get( jobID )
            #
            #        occupiedSlots[ job.job_details.host_id ] += job.slots
            #
            #        # set checked flag
            #        jobHistory.checked = True
            for finishedJob in finishedJobs:
                job = finishedJob.job
                loggerWrapper.write( "   Job [{j}] has finished. Free occupied slots on host.".format(j=job.id ), logCategory='checkingJobs' )

                occupiedSlots[ job.job_details.host_id ] += job.slots

                dbconnection.delete( finishedJob )

            # free occupied slots on host
            for h in occupiedSlots:
                dbconnection.query( db.HostSummary ).\
                                    filter( db.HostSummary.host_id==h ).\
                                    update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - occupiedSlots[ h ] } )

            dbconnection.commit()

            # releas lock
            #loggerWrapper.write( "  ... release lock")
            #self.Lock.release()
            
            self.checkingFinishedJobs = False

    def printStatus(self, returnString=False):
        """!@brief print status of server to stdout if not outSream is given

        @param returnString (boolean) return formatted status instead of passing it to logger
        """

        if returnString or not self.printingStatus:
            if not returnString:
                self.printingStatus=True
            
            dbconnection = hDBConnection()

            # get all number of jobs for each status type
            #print "QUERY",dbconnection.query( db.JobStatus.name, func.count('*') ).join( db.JobDetails, db.JobDetails.job_status_id==db.JobStatus.id ).group_by( db.JobStatus.name )
            t1 = datetime.now()
            counts = dict( dbconnection.query( db.JobStatus.name, func.count('*') ).join( db.JobDetails, db.JobDetails.job_status_id==db.JobStatus.id ).group_by( db.JobStatus.name ).all() )
            t2 = datetime.now()
            print "Job Query in {dt}".format(dt=str(t2-t1))

            if not counts:
                # no jobs so far in the database
                counts = {}

            t1 = datetime.now()
            slotInfo = dbconnection.query( func.count('*'),
                                           func.sum( db.Host.max_number_occupied_slots ), 
                                           func.sum( db.HostSummary.number_occupied_slots ) ).select_from( db.Host ).join( db.HostSummary, db.HostSummary.host_id==db.Host.id ).filter( db.HostSummary.active==True ).one()
            t2 = datetime.now()
            print "Host Query in {dt}".format(dt=str(t2-t1))
            print 

            if slotInfo[0]==0:
                slotInfo = (0, 0, 0)

            href = "--------------------------------------------------"
            info = "[{t}] STATUS OF TASKDISPATCHER ON {h}:{p}".format(t=t1, h=self.host, p=self.port)
            status = ""
            status += "{s:>20} : {value}\n".format(s="active cluster", value=self.active )
            status += "{s:>20} : {value}\n".format(s="active hosts", value=slotInfo[0] )
            status += "{s:>20} : {value}\n".format(s="occupied slots", value="{occupied} / {total}".format(occupied=slotInfo[2],total=slotInfo[1]) )
            status += "{s:>20} : {value}\n".format(s="waiting jobs", value=counts.get('waiting',0) )
            status += "{s:>20} : {value}\n".format(s="pending jobs", value=counts.get('pending',0) )
            status += "{s:>20} : {value}\n".format(s="running jobs", value=counts.get('running',0) )
            status += "{s:>20} : {value}".format(s="finished jobs", value=counts.get('finished',0) )

            dbconnection.remove()

            statusString = "{info}\n{href}\n{status}\n{href}\n".format(href=href, info=info, status=status)
            if not returnString:
                loggerWrapper.write( statusString, logCategory="status" )
                self.printingStatus=False
            else:
                return statusString

        

        
# The RequestHandler handles an incoming request.
class TaskDispatcherRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, clientAddress, TD):
        self.TD = TD
        self.currThread = threading.currentThread()

        # add instance attributes
        self.currThread.description = ""
        self.currThread.date = datetime.now()
        
        SocketServer.BaseRequestHandler.__init__(self, request, clientAddress, self.TD)

    def handle(self):
        ##(sread, swrite, sexc) = select.select([self.request], [], [], None)

        self.currThread.setName( "Thread [{t}]".format(t=str(datetime.now() ) ) )
        
        # create a hSocket-instance
        requestSocket = hSocket( sock=self.request, 
                                 serverSideSSLConn=True,
                                 catchErrors=False, 
                                 EOCString=self.TD.EOCString,
                                 timeout=10)
        try:
            receivedStr = requestSocket.recv()
        except socket.timeout:
            logger.warn( "Timeout while reading from socket. Skip" )
            return

        #self.currThread.setName( "Thread [{t}]: {s}{dots}".format(t=str(datetime.now()),
        #                                                          s=receivedStr[:30],
        #                                                          dots="..." if len(receivedStr[:30])==30 else "" ) )
        self.currThread.setName( "(command) {s}{dots}".format(t=str(datetime.now()),
                                                                  s=receivedStr[:30],
                                                                  dots="..." if len(receivedStr[:30])==30 else "" ) )
        self.currThread.description = receivedStr
                                     
        
        if receivedStr!="check":
            loggerWrapper.write( "NEW REQUEST: {r1}{dots}{r2}".format(t=str(datetime.now()),
                                                                      r1=receivedStr[:40] if len(receivedStr)>80 else receivedStr,
                                                                      dots="..." if len(receivedStr)>80 else "",
                                                                      r2=receivedStr[-40:] if len(receivedStr)>80 else "" ) )

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

        if receivedStr!="check":
            loggerWrapper.write( "REQUEST PROCESSED IN {dt}s.".format(dt=str(t2-t1) ) )

        
    def finish(self):
        """! @brief Send jobs to vacant hosts and cleanup handler afterwards

        Check database if there are waiting jobs, check user, find vacant host, and send job to associated tms of user.
        """

        #self.TD.updateJobStatus()
        if self.TD.active and not self.TD.sendingJobs:
            try:
                self.TD.sendingJobs = True

                ## get next job, find vacant host and send job

                dbconnection = hDBConnection()

                loggerWrapper.write( "----------------------------", logCategory="sendingJobs" )

                t1 = datetime.now()

                # get number of free slots
                slotInfo = dbconnection.query( func.count('*'),
                                               func.sum( db.Host.max_number_occupied_slots ), 
                                               func.sum( db.HostSummary.number_occupied_slots ) ).select_from( db.Host ).join( db.HostSummary, db.HostSummary.host_id==db.Host.id ).filter( db.HostSummary.active==True ).one()

                if slotInfo[1]!=None and slotInfo[2]!=None:
                    freeSlots = slotInfo[1] - slotInfo[2]
                else:
                    freeSlots = 0

                loggerWrapper.write( "Free slots: {n}".format(n=freeSlots), logCategory="sendingJobs" )

                if freeSlots>0:

                    loggerWrapper.write( "Get jobs to be executed ...".format(n=freeSlots), logCategory="sendingJobs" )

                    # get next jobs ready to be sent to associate tms
                    #jobIDs = self.TD.getNextJobs( numJobs=freeSlots, excludedJobIDs=self.TD.lockedJobs )
                    #jobIDs = self.TD.getNextJobs( numJobs=freeSlots )
                    jobIDs = self.TD.jobScheduler.next( numJobs=freeSlots )

                    if jobIDs:
                        loggerWrapper.write( "... got {n} jobs to be executed.".format(n=len(jobIDs) ), logCategory="sendingJobs" )

                        # iterate over all jobs
                        # reduce freeSlots accordingly after job has been submitted
                        for idx,jobID in enumerate(jobIDs):
                            # add job to locked jobs, i.e., they are currently processed
                            #self.TD.lockedJobs.add( jobID )

                            # get Job instance
                            job = dbconnection.query( db.Job ).get( jobID )
                            user = job.user

                            loggerWrapper.write( "  next job {idx}/{N} is {i} of user {u}".format( idx=idx+1, N=len(jobIDs), i=jobID, u=user.name ) )

                            # get excluded hosts
                            excludedHosts = json.loads( job.excluded_hosts )

                            # get vacant host which has the necessary number of free slots
                            vacantHost = self.TD.getVacantHost( job.slots, excludedHosts=set( excludedHosts ) )

                            if vacantHost:
                                loggerWrapper.write( "    run job {j} of user {u} on {h}".format(j=jobID, u=user.name, h=vacantHost[1] ), logCategory="sendingJobs" )

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
                                          update( { db.JobDetails.job_status_id: self.TD.databaseIDs['pending'],
                                                    db.JobDetails.host_id: vacantHost[0] } )


                                        # reduce slots in host
                                        dbconnection.query( db.HostSummary ).\
                                          filter( db.HostSummary.host_id==vacantHost[0] ).\
                                          update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots+job.slots })

                                        # set history
                                        jobHistory = db.JobHistory( job=job,
                                                                    job_status_id = self.TD.databaseIDs['pending'] )

                                        # remove job from waiting list
                                        dbconnection.query( db.WaitingJob ).filter( db.WaitingJob.job_id==jobID ).delete()
                                        
                                        dbconnection.introduce( jobHistory )

                                        dbconnection.commit()

                                        # set job to tms
                                        jsonObj = json.dumps( { 'jobID': jobID,
                                                                'hostID': vacantHost[0],
                                                                'hostName': vacantHost[1] })
                                        sock.send( 'runjob:{j}'.format( j=jsonObj ) )

                                        freeSlots -= job.slots

                                except socket.error,msg:
                                    loggerWrapper.write( "... failed [{h}:{p}]: {m}".format(h=user.tms_host, p=user.tms_port, m=msg), logCategory="sendingJobs" )

                            loggerWrapper.write( "  free slots: {s}".format(s=freeSlots), logCategory="sendingJobs")

                            # remove job from locked ids
                            #self.TD.lockedJobs.remove( jobID )

                            if freeSlots < 1:
                                break
                        loggerWrapper.write( "done.", logCategory="sendingJobs" )
                    else:
                        loggerWrapper.write( '... no jobs.', logCategory="sendingJobs" )


                t2 = datetime.now()
                loggerWrapper.write( "... done in {dt}s.".format(dt=str(t2-t1) ), logCategory="sendingJobs" )
            except:
                # error handling
                traceback.print_exc(file=sys.stdout)
            finally:
                self.TD.sendingJobs = False

        # print status
        #self.TD.printStatus()
        
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
        self.commands["CHECK"] = hCommand( command_name = "check",
                                          regExp = '^check$',
                                          help = "check if there are free slots and jobs to run.")
        self.commands["HELP"] = hCommand( command_name = "help",
                                          regExp = "^help$",
                                          help = "return help")
        self.commands["TOPICHELP"] = hCommand( command_name = "help",
                                               arguments = "<TOPIC>",
                                               regExp = "^help:(.*)",
                                               help = "return help about topic")
        self.commands["LSTHREADS"] = hCommand(command_name = "lsthreads",
                                              regExp = "^lsthreads$",
                                              help = "return list of active threads")
        self.commands["LSTHREAD"] = hCommand(command_name = "lsthread",
                                             regExp = "^lsthread:(.*)",
                                             arguments = '<THREAD NAME>',
                                             help = "return details of thread with specified name (see lsthread)")
        self.commands["LOADLOGGERCONFIG"] = hCommand( command_name = "loadloggerconfig",
                                                      regExp = "^loadloggerconfig$",
                                                      help = "load logger config")
        self.commands["STATUS"] = hCommand( command_name = "status",
                                            regExp = "^status$",
                                            help = "get status of taskmanager")
        self.commands["ACTIVATECLUSTER"] = hCommand( command_name = "activatecluster",
                                                     regExp = "^activatecluster$",
                                                     help = "activate cluster")
        self.commands["DEACTIVATECLUSTER"] = hCommand( command_name = "deactivatecluster",
                                                       regExp = "^deactivatecluster$",
                                                       help = "deactivate cluster")
        self.commands["LSCLUSTER"] = hCommand( command_name = "lscluster",
                                                       regExp = "^lscluster$",
                                                       help = "show cluster details")
        self.commands["UPDATELOAD"] = hCommand( command_name = "updateload",
                                              regExp = "^updateload$",
                                              help = "update load of hosts")
        self.commands["CHECKDATABASE"] = hCommand( command_name = "checkdatabase",
                                                   regExp = "^checkdatabase$",
                                                   help = "check database for waiting jobs")
        self.commands["CHECKSLOTS"] = hCommand( command_name = "checkslots",
                                                   regExp = "^checkslots$",
                                                   help = "check occupied slots")
        self.commands["ENABLEUSER"] = hCommand( command_name = "enableuser",
                                                 arguments = "<USER>",
                                                 regExp = "^enableuser:(.*)",
                                                 help = "enable user.")
        self.commands["DISABLEUSER"] = hCommand( command_name = "disableuser",
                                                 arguments = "<USER>",
                                                 regExp = "^disableuser:(.*)",
                                                 help = "disable user.")
        self.commands["LSUSERS"] = hCommand( command_name = "lsusers",
                                                 regExp = "^lsusers$",
                                                 help = "show all users.")
        self.commands["LSUSER"] = hCommand( command_name = "lsuser",
                                            arguments = "<USER NAME>",
                                            regExp = "^lsusers:(.*)$",
                                            help = "show details about user.")
        self.commands["SETINTERVALCHECKTD"] = hCommand( command_name = "setintervalchecktd",
                                                        arguments = "<TIME>",
                                                        regExp = "^setintervalchecktd:(.*)",
                                                        help = "set interval in seconds for loop pinging TaskDispatcher")
        self.commands["SETINTERVALPRINTSTATUS"] = hCommand( command_name = "setintervalprintstatus",
                                                 arguments = "<TIME>",
                                                 regExp = "^setintervalprintstatus:(.*)",
                                                 help = "set interval in seconds for loop printing status")
        self.commands["SETINTERVALCHECKFINISHEDJOBS"] = hCommand( command_name = "setintervalcheckfinishedjobs",
                                                 arguments = "<TIME>",
                                                 regExp = "^setintervalcheckfinishedjobs:(.*)",
                                                 help = "set interval in seconds for loop checking finished jobs")
        self.commands["SETINTERVALUPDATEHOSTLOAD"] = hCommand( command_name = "setintervalupdatehostload",
                                                 arguments = "<TIME>",
                                                 regExp = "^setintervalupdatehostload:(.*)",
                                                 help = "set interval in seconds for loop updating host load")
        self.commands["ADDJOB"] = hCommand(command_name = 'addjob',
                                           arguments = "<jsonStr>",
                                           regExp = '^addjob:(.*)',
                                           help = "add job from user to database")
        self.commands["ADDJOBS"] = hCommand(command_name = 'addjobs',
                                           arguments = "<jsonStr>",
                                           regExp = '^addjobs:(.*)',
                                           help = "add jobs from user to database")
        self.commands["GETTDSTATUS"] = hCommand( command_name = "gettdstatus",
                                                 regExp = "^gettdstatus$",
                                                 help = "return status info of the TaskDispatcher")
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

        self.commands["LAJOB"] = hCommand(command_name = 'lajob',
                                          arguments = "<jobID>",
                                          regExp = 'lajob:(.*)',
                                          help = "return job info about job with given jobID")
        
        self.commands["FINDJOBS"] = hCommand(command_name = 'findjobs',
                                            arguments = "<matchString>",
                                            regExp = 'findjobs:(.*)',
                                            help = "return all jobs which match the search string in command, info text and group.")

        self.commands["LSNEXTJOBS"] = hCommand(command_name = 'lsnextjobs',
                                               arguments = "<number>",
                                               regExp = '^lsnextjobs:(.*)',
                                               help = "return next jobs which will be send to cluster")

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

        self.commands["SETALLPJOBSASWAITING"] = hCommand(command_name = 'setallpjobsaswaiting',
                                                         regExp = '^setallpjobsaswaiting$',
                                                         help = "set all pending jobs as waiting. free occupied slots on hosts.")
        
        self.commands["SETALLPJOBSASFINISHED"] = hCommand(command_name = 'setallpjobsasfinished',
                                                         regExp = '^setallpjobsasfinished$',
                                                         help = "set all pending jobs as finished. free occupied slots on hosts.")
        
        self.commands["SETALLRJOBSASWAITING"] = hCommand(command_name = 'setallrjobsaswaiting',
                                                         regExp = '^setallrjobsaswaiting$',
                                                         help = "set all running jobs as waiting. free occupied slots on hosts.")
        
        self.commands["SETALLRJOBSASFINISHED"] = hCommand(command_name = 'setallrjobsasfinished',
                                                         regExp = '^setallrjobsasfinished$',
                                                         help = "set all running jobs as finished. free occupied slots on hosts.")

        self.commands["RMJOBS"] = hCommand(command_name = 'rmjobs',
                                           arguments = "<username>",
                                           regExp = '^rmjobs:(.*)',
                                           help = "remove all jobs of user from database")

        self.commands["RMWJOBS"] = hCommand(command_name = 'rmwjobs',
                                            arguments = "<username>",
                                            regExp = '^rmwjobs:(.*)',
                                            help = "remove all waiting jobs of user from database")
        

        
    def process(self, requestStr, request, TD):
        """Process a requestStr"""

        dbconnection = hDBConnection()
        
        #####################
        # check if request is a known command

        if self.commands["PING"].re.match(requestStr):
            request.send("pong")
            
        if self.commands["CHECK"].re.match(requestStr):
            request.send("checked")
            
        elif self.commands["HELP"].re.match(requestStr):
            help = []
            help.append( "Commands known by the TaskDispatcher:" )
            # iterate over all commands defined in self.commands
            help.extend( renderHelp( sorted(self.commands.keys()), self.commands ) )
            
            request.send( '\n'.join( help ) )
            
        elif self.commands["LOADLOGGERCONFIG"].re.match(requestStr):
            loggerWrapper.load()
            request.send( 'done.' )
            
        #  get list of active threads
        elif self.commands["LSTHREADS"].re.match(requestStr):
            threadList = [ "{idx:3d} - {name}".format(idx=idx,name=t.getName()) for idx,t in enumerate(threading.enumerate() ) ]
            request.send( '\n'.join( threadList ) )
            
        elif self.commands["LSTHREAD"].re.match(requestStr):
            c = self.commands["LSTHREAD"]
            
            threadName = c.re.match( requestStr ).groups()[0]
            
            try:
                thread = next( t for t in threading.enumerate() if t.getName()==threadName )
            except StopIteration:
                # not found
                return ""

            response = "Thread details\n"
            response += "----------------------\n"
            response += "{s:>20} : {value}\n".format(s="name", value=thread.getName() )
            response += "{s:>20} : {value}\n".format(s="description", value=thread.description )
            response += "{s:>20} : {value}\n".format(s="started at", value=str(thread.date) )
            response += "{s:>20} : {value}\n".format(s="running since", value=str(datetime.now()-thread.date) )

            request.send( response )
            

        elif self.commands["STATUS"].re.match( requestStr ):
            status = TD.printStatus( returnString=True )

            request.send( status )
            
        elif self.commands["ACTIVATECLUSTER"].re.match( requestStr ):
            # set active status to True
            TD.active = True

            request.send( 'activated.' )

        elif self.commands["DEACTIVATECLUSTER"].re.match( requestStr ):
            # set active status to False
            TD.active = False
            
            request.send( 'deactivated.' )

        elif self.commands["LSCLUSTER"].re.match( requestStr ):
            # show cluster
            hosts = dbconnection.query( db.Host ).all()

            response = "cluster is {s}\n".format( s='active' if TD.active else 'not active' )
            response += "------------------------\n"
            
            for idx,host in enumerate(hosts):
                try:
                    load = host.host_load[-1].loadavg_1min
                except:
                    load = 'n.a.'

                hostInfo = { 'i': idx,
                             'name': host.full_name,
                             'status': 'active' if host.host_summary.active else 'reachable' if host.host_summary.reachable else 'available' if host.host_summary.available else 'not available',
                             'occupiedSlots': host.host_summary.number_occupied_slots,
                             'freeSlots': host.max_number_occupied_slots - host.host_summary.number_occupied_slots,
                             'maxSlots': host.max_number_occupied_slots,
                             'load': load
                             }
                response += "{i} - [host:{name}] [status:{status}] [free slots:{freeSlots}/{maxSlots}] [load:{load}]\n".format( **hostInfo )

            if response:
                request.send( response )
            else:
                request.send( "no hosts in cluster." )
                
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
            
        elif self.commands["CHECKSLOTS"].re.match( requestStr ):
            counts = dict( dbconnection.query( db.Host.id, func.sum( db.Job.slots ) ).\
                           join( db.JobDetails, db.JobDetails.host_id==db.Host.id ).\
                           join( db.JobStatus, db.JobDetails.job_status_id==db.JobStatus.id ).\
                           join( db.Job, db.JobDetails.job_id==db.Job.id ).\
                           filter( db.JobStatus.id.in_( [ TD.databaseIDs['pending'],TD.databaseIDs['running'] ] ) ).\
                           group_by( db.Host.full_name ).all() )

            for h in dbconnection.query( db.HostSummary ):
                h.number_occupied_slots = counts.get( h.id, 0)

            dbconnection.commit()
            
            request.send( "number of occupied slots have been updated" )
            
        elif self.commands['ENABLEUSER'].re.match( requestStr ):
            c = self.commands["ENABLEUSER"]
            
            user = c.re.match( requestStr ).groups()[0]

            try:
                dbconnection.query( db.User ).filter( db.User.name==user ).update( {db.User.enabled: True} )
                dbconnection.commit()
                
                request.send('done.')
            except:
                request.send('failed.')

        elif self.commands['DISABLEUSER'].re.match( requestStr ):
            c = self.commands["DISABLEUSER"]
            
            user = c.re.match( requestStr ).groups()[0]

            try:
                dbconnection.query( db.User ).filter( db.User.name==user ).update( {db.User.enabled: False} )
                dbconnection.commit()
                
                request.send('done.')
            except:
                request.send('failed.')
                
        elif self.commands['LSUSERS'].re.match( requestStr ):
            
            try:
                users = dbconnection.query( db.User ).all()

                response = ""
                for user in users:
                    response += "{name} ({e})  [TMS] {tmsHost}:{tmsPort}\n".format( name = user.name,
                                                                                                                  e = 'enabled' if user.name else 'disabled',
                                                                                                                  tmsHost = user.tms_host,
                                                                                                                  tmsPort = user.tms_port )

                
                
                request.send( response )
            except:
                request.send('failed.')
            
        elif self.commands['SETINTERVALCHECKTD'].re.match( requestStr ):
            c = self.commands["SETINTERVALCHECKTD"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopCheckTD = interval

            request.send('done.')
            
        elif self.commands['SETINTERVALPRINTSTATUS'].re.match( requestStr ):
            c = self.commands["SETINTERVALPRINTSTATUS"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopPrintStatusInterval = interval

            request.send('done.')
            
        elif self.commands['SETINTERVALCHECKFINISHEDJOBS'].re.match( requestStr ):
            c = self.commands["SETINTERVCHECKFINISHEDJOBS"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopCheckFinishedJobsInterval = interval

            request.send('done.')
            
        elif self.commands['SETINTERVALUPDATEHOSTLOAD'].re.match( requestStr ):
            c = self.commands["SETINTERVALUPDATEHOSTLOAD"]
            
            interval = int( c.re.match( requestStr ).groups()[0] )

            # update value
            TD.loopUpdateHostLoadInterval = interval

            request.send('done.')
        elif self.commands['ACTIVATEHOST'].re.match( requestStr ):
            c = self.commands['ACTIVATEHOST']

            host = c.re.match( requestStr ).groups()[0]

            TD.activateHost( host )

            request.send( "host {h} has been activated".format(h=host) )

            
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
        #        loggerWrapper.write( 'Added new user {u}'.format(u=user) )

        elif self.commands['DEACTIVATEHOST'].re.match( requestStr ):
            c = self.commands['DEACTIVATEHOST']

            host = c.re.match( requestStr ).groups()[0]

            TD.deactivateHost( host )
            
            request.send( "host {h} has been deactivated".format(h=host) )
            
        elif self.commands["ADDJOB"].re.match( requestStr ):
            c = self.commands["ADDJOB"]

            jsonObj = c.re.match( requestStr ).groups()[0]
            jsonObj = json.loads( jsonObj )

            command = jsonObj['command']
            slots = int(jsonObj.get('slots', 1))
            infoText = jsonObj.get('infoText','')
            group = jsonObj.get('group','')
            stdout = jsonObj.get('stdout','')
            stderr = jsonObj.get('stdin','')
            logfile = jsonObj.get('logfile','')
            shell = jsonObj['shell']
            priorityValue = int(jsonObj.get('priority',0))
            estimatedTime = float(jsonObj.get('estimatedTime',0))
            estimatedMemory = float(jsonObj.get('estimatedMemory',0))
            excludedHosts = filter(None, jsonObj.get("excludedHosts","").split(',') )
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
                    loggerWrapper.write( 'Unknown user {u}'.format(u=user) )
                    request.send( 'Unknown user' )
                    
            except:
                traceback.print_exc(file=sys.stderr)

            # check excluded hosts
            excludedHostsList = []
            for h in excludedHosts:
                try:
                    dbconnection.query( db.Host ).filter( db.Host.full_name==h ).one()
                    excludedHostsList.append( h )
                except NoResultFound:
                    # do not consider this host
                    continue

            # priority value is supposed to be between  0 and 127
            priorityValue = 127 if priorityValue > 127 else 0 if priorityValue<0 else priorityValue
            # get database id of priority
            try:
                priority = dbconnection.query( db.Priority ).filter( db.Priority.value==priorityValue ).one()
            except NoResultFound:
                priority = db.Priority( value=priorityValue )
                dbconnection.introduce( priority )

            # create database entry for the new job
            newJob = db.Job( user_id=user_id,
                             command=command,
                             slots=slots,
                             info_text=infoText,
                             group=group,
                             shell=shell,
                             stdout=stdout,
                             stderr=stderr,
                             logfile=logfile,
                             priority=priority,
                             estimated_time=estimatedTime,
                             estimated_memory=estimatedMemory,
                             excluded_hosts=json.dumps( excludedHostsList ) )

            # set jobstatus for this job
            jobDetails = db.JobDetails( job=newJob,
                                        job_status_id=TD.databaseIDs['waiting'] )

            # calculate a priority value
            pValue = priority.value
            
            # add to waiting job
            waitingJob = db.WaitingJob( job=newJob,
                                        user_id=user_id,
                                        priorityValue=pValue )

            # set history
            jobHistory = db.JobHistory( job=newJob,
                                        job_status_id = TD.databaseIDs['waiting'] )
            
            dbconnection.introduce( newJob, jobDetails, jobHistory, waitingJob )
            
            dbconnection.commit()

            loggerWrapper.write( 'Added new job with id {i}'.format( i=newJob.id ) )
            
            request.send( str(newJob.id) )
            
            
        elif self.commands["ADDJOBS"].re.match( requestStr ):
            c = self.commands["ADDJOBS"]

            jsonObj = c.re.match( requestStr ).groups()[0]
            jsonObj = json.loads( jsonObj )

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
                    loggerWrapper.write( 'Unknown user {u}'.format(u=user) )
                    request.send( 'Unknown user' )

            except:
                traceback.print_exc(file=sys.stderr)

            jobIDs = []

            loggerWrapper.write( "Add {n} jobs ...".format(n=len(jsonObj['jobs'])) )
            
            # iterate over all jobs
            for idx,job in enumerate(jsonObj['jobs']):
                command = job['command']
                slots = int(job['slots'])
                infoText = job.get('infoText','')
                group = job.get('group','')
                stdout = job.get('stdout','')
                stderr = job.get('stdin','')
                logfile = job.get('logfile','')
                shell = job['shell']
                excludedHosts = job.get("excludedHosts","").split(',')
                priorityValue = job.get('priority',0)
                estimatedTime = int(job.get("estimatedTime",0))
                estimatedMemory = int(job.get("estimatedmEMORY",0))
                
                # check excluded hosts
                excludedHostsList = []
                for h in excludedHosts:
                    try:
                        dbconnection.query( db.Host ).filter( db.Host.full_name==h ).one()
                        excludedHostsList.append( h )
                    except NoResultFound:
                        # do not consider this host
                        continue

                # priority value is supposed to be between  0 and 127
                priorityValue = 127 if priorityValue > 127 else 0 if priorityValue<0 else priorityValue
                # get database id of priority
                try:
                    priority = dbconnection.query( db.Priority ).filter( db.Priority.value==priorityValue ).one()
                except NoResultFound:
                    priority = db.Priority( value=priorityValue )
                    dbconnection.introduce( priority )
                    
                # create database entry for the new job
                newJob = db.Job( user_id=user_id,
                                 command=command,
                                 slots=int(slots),
                                 priority=priority,
                                 info_text=infoText,
                                 group=group,
                                 shell=shell,
                                 stdout=stdout,
                                 stderr=stderr,
                                 logfile=logfile,
                                 excluded_hosts=json.dumps( excludedHostsList ) )

                # set jobstatus for this job
                jobDetails = db.JobDetails( job=newJob,
                                            job_status_id=TD.databaseIDs['waiting'] )

                # add as waiting job
                waitingJob = db.WaitingJob( job=newJob,
                                            user_id=user_id,
                                            priorityValue=priority.value )	# calculate a priority value

                # set history
                jobHistory = db.JobHistory( job=newJob,
                                            job_status_id = TD.databaseIDs['waiting'] )

                dbconnection.introduce( newJob, jobDetails, waitingJob, jobHistory )

                dbconnection.commit()

                loggerWrapper.write( '  {idx}/{n}: added job with id {i}'.format( idx=idx, n=len(jsonObj['jobs']), i=newJob.id ) )

                jobIDs.append( str(newJob.id) )
                               
            request.send( json.dumps( jobIDs ) )
            
            
        elif self.commands["GETTDSTATUS"].re.match( requestStr ):

            dbconnection = hDBConnection()

            numHosts = dbconnection.query( db.HostSummary ).filter( and_(db.HostSummary.available==True,
                                                                         db.HostSummary.reachable==True,
                                                                         db.HostSummary.active==True) ).count() 
            
            slotInfo = dbconnection.query( func.count('*'),
                                           func.sum( db.Host.max_number_occupied_slots ), 
                                           func.sum( db.HostSummary.number_occupied_slots ) ).select_from( db.Host ).join( db.HostSummary, db.HostSummary.host_id==db.Host.id ).filter( db.HostSummary.active==True ).one()

            if slotInfo[0]==0:
                slotInfo = (0, 0, 0)

            dbconnection.remove()
            
            response = { 'activity status': TD.active,
                         'active hosts': numHosts,
                         'total slots': int(slotInfo[1]),
                         'occupied slots': int(slotInfo[2]) }

            request.send( json.dumps(response) )

        elif self.commands["LSWJOBS"].re.match( requestStr ):
            # connect to database
            dbconnection = hDBConnection()
            
            wJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['waiting'] ).all()

            response = ""
            for idx,job in enumerate(wJobs):
                response += "{i} - [jobid:{id}] [user:{user}] [status:waiting since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                            id=job.id,
                                                                                                                                                            user=job.user.name,
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
        elif self.commands["LSPJOBS"].re.match(requestStr):
            # connect to database
            dbconnection = hDBConnection()
            
            pJobs = dbconnection.query( db.Job,db.JobDetails ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['pending'] ).all()

            response = ""
            for idx,(job,jobDetails) in enumerate(pJobs):
                response += "{i} - [jobid:{id}] [user:{user}] [status:pending on {host} since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                                      id=job.id,
                                                                                                                                                                      user=job.user.name,
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
        elif self.commands["LSRJOBS"].re.match(requestStr):
            # connect to database
            dbconnection = hDBConnection()
            
            pJobs = dbconnection.query( db.Job, db.JobDetails ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['running'] ).all()

            response = ""
            for idx,(job,jobDetails) in enumerate(pJobs):
                response += "{i} - [jobid:{id}] [user:{user}] [status:running on {host} since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                                      id=job.id,
                                                                                                                                                                      user=job.user.name,
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
        elif self.commands["LSFJOBS"].re.match(requestStr):
            # connect to database
            dbconnection = hDBConnection()

            rJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['finished'] ).all()

            response = ""
            for idx,job in enumerate(rJobs):
                response += "{i} - [jobid:{id}] [user:{user}] [status:finished since {t}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                             id=job.id,
                                                                                                                                                             user=job.user.name,
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
        elif self.commands["LAJOB"].re.match(requestStr):
            c = self.commands["LAJOB"]

            jobID = c.re.match(requestStr).groups()[0]

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

                try:
                    response += "{s:>20} : {value}\n".format(s="host", value=job.job_details.host.short_name )
                except:
                    response += "{s:>20} : {value}\n".format(s="host", value="None" )
                response += "{s:>20} : {value}\n".format(s="pid", value=job.job_details.pid )
                response += "{s:>20} : {value}\n".format(s="return code", value=job.job_details.return_code )

                request.send( response )
            else:
                request.send("unkown job.")


        #  get all jobs with match the search string
        elif self.commands["FINDJOBS"].re.match(requestStr):
            c = self.commands["FINDJOBS"]

            matchString = c.re.match(requestStr).groups()[0]

            # connect to database
            dbconnection = hDBConnection()

            jobs = dbconnection.query( db.Job ).filter( or_(db.Job.command.ilike( '%{s}%'.format(s=matchString) ),
                                                            db.Job.info_text.ilike( '%{s}%'.format(s=matchString) ),
                                                            db.Job.group.ilike( '%{s}%'.format(s=matchString) ) ) ).all()
            

            response = ""
            for idx,job in enumerate(jobs):
                response += "{i} - [jobid:{id}] [user:{user}] [status:{status}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                   id=job.id,
                                                                                                                                                   user=job.user.name,
                                                                                                                                                   status=job.job_details.job_status.name,
                                                                                                                                                   group=job.group,
                                                                                                                                                   info=job.info_text,
                                                                                                                                                   command=job.command[:30],
                                                                                                                                                   dots="..." if len(job.command)>30 else "" )

            if response:
                request.send( response )
            else:
                request.send("no jobs found")

            dbconnection.remove()

            

        elif self.commands["LSNEXTJOBS"].re.match(requestStr):
            c = self.commands["LSNEXTJOBS"]

            number = int( c.re.match(requestStr).groups()[0] )

            #jobs = TD.getNextJobs( numJobs=number, returnInstances=True )
            jobs = TD.jobScheduler.next( numJobs=number, returnInstances=True )

            response = ""
            for idx,wJob in enumerate(jobs):
                job = wJob.job
                response += "{i} - [jobid:{id}] [user:{user}] [status:{status}] [group:{group}] [info:{info}] [command:{command}{dots}]\n".format( i=idx,
                                                                                                                                                   id=job.id,
                                                                                                                                                   user=job.user.name,
                                                                                                                                                   status=job.job_details.job_status.name,
                                                                                                                                                   group=job.group,
                                                                                                                                                   info=job.info_text,
                                                                                                                                                   command=job.command[:30],
                                                                                                                                                   dots="..." if len(job.command)>30 else "" )

            if response:
                request.send( response )
            else:
                request.send("no jobs found to send")

            dbconnection.remove()
            
            
        elif self.commands["SETALLPJOBSASWAITING"].re.match( requestStr ):
            # get all pending jobs
            pJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['pending'] ).all()

            # get occupied slots of each host
            slots = dict( dbconnection.query( db.HostSummary.host_id, db.HostSummary.number_occupied_slots ).all() )
            
            occupiedSlots = defaultdict( int )
            for job in pJobs:
                occupiedSlots[ job.job_details.host_id ] += job.slots
                
                # set job as waiting
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: TD.databaseIDs['waiting'] } )

                # add to waiting jobs
                wJob = db.WaitingJob( job=job,
                                      user_id=job.user_id,
                                      priorityValue=job.priority.value )
                
                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = TD.databaseIDs['waiting'] )

                dbconnection.introduce( jobHistory, wJob )

            dbconnection.commit()
            
            # free occupied slots from host
            for h in occupiedSlots:
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==h ).\
                  update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - occupiedSlots[ h ] } )

            dbconnection.commit()

            request.send( "set {n} jobs as waiting".format(n=len(pJobs)) )

        elif self.commands["SETALLRJOBSASWAITING"].re.match( requestStr ):
            rJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['running'] ).all()
            slots = dict( dbconnection.query( db.HostSummary.host_id, db.HostSummary.number_occupied_slots ).all() )

            occupiedSlots = defaultdict( int )
            for job in rJobs:
                occupiedSlots[ job.job_details.host_id ] += job.slots
                
                # set job as waiting
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: TD.databaseIDs['waiting'] } )

                # add to waiting jobs
                wJob = db.WaitingJob( job=job,
                                      user_id=job.user_id,
                                      priorityValue=priority.value )	# calculate a priority value
                
                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = TD.databaseIDs['waiting'] )

                dbconnection.introduce( jobHistory, wJob )

            # free occupied slots from host
            for h in occupiedSlots:
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==h ).\
                  update( { db.HostSummary.number_occupied_slots: db.HostSummary.number_occupied_slots - occupiedSlots[ h ] } )

            dbconnection.commit()
            
            request.send( "set {n} jobs as waiting".format(n=len(rJobs)) )
            
        elif self.commands["SETALLPJOBSASFINISHED"].re.match( requestStr ):
            pJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['pending'] ).all()
            slots = dict( dbconnection.query( db.HostSummary.host_id, db.HostSummary.number_occupied_slots ).all() )

            occupiedSlots = defaultdict( int )
            for job in pJobs:
                occupiedSlots[ job.job_details.host_id ] += job.slots

                # set job as finished
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: TD.databaseIDs['finished'] } )

                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = TD.databaseIDs['finished'],
                                            checked = True )

                dbconnection.introduce( jobHistory )

            # free occupied slots from host
            for h in occupiedSlots:
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==h ).\
                  update( { db.HostSummary.number_occupied_slots: slots[ h ] - occupiedSlots[ h ] } )

            dbconnection.commit()

            request.send( "set {n} jobs as finished".format(n=len(pJobs)) )
            
        elif self.commands["SETALLRJOBSASFINISHED"].re.match( requestStr ):
            rJobs = dbconnection.query( db.Job ).join( db.JobDetails ).filter( db.JobDetails.job_status_id==TD.databaseIDs['running'] ).all()
            slots = dict( dbconnection.query( db.HostSummary.host_id, db.HostSummary.number_occupied_slots ).all() )

            occupiedSlots = defaultdict( int )
            for job in rJobs:
                occupiedSlots[ job.job_details.host_id ] += job.slots

                # set job as finished
                dbconnection.query( db.JobDetails.job_id ).\
                  filter( db.JobDetails.job_id==job.id ).\
                  update( {db.JobDetails.job_status_id: TD.databaseIDs['finished'] } )

                # set history
                jobHistory = db.JobHistory( job=job,
                                            job_status_id = TD.databaseIDs['finished'],
                                            checked = True )

                dbconnection.introduce( jobHistory )

            # free occupied slots from host
            for h in occupiedSlots:
                dbconnection.query( db.HostSummary ).\
                  filter( db.HostSummary.host_id==h ).\
                  update( { db.HostSummary.number_occupied_slots: slots[ h ] - occupiedSlots[ h ] } )

            dbconnection.commit()
            
            request.send( "set {n} jobs as finished".format(n=len(rJobs)) )
            
        elif self.commands["RMJOBS"].re.match( requestStr ):
            c = self.commands["RMJOBS"]
            
            user = c.re.match( requestStr ).groups()[0]

            try:
                userInstance = dbconnection.query( db.User ).filter( db.User.name==user ).one()

                counts = dict( dbconnection.query( db.Host.id, func.sum( db.Job.slots ) ).\
                               join( db.JobDetails, db.JobDetails.host_id==db.Host.id ).\
                               join( db.JobStatus, db.JobDetails.job_status_id==db.JobStatus.id ).\
                               join( db.Job, db.JobDetails.job_id==db.Job.id ).\
                               filter( db.Job.user==userInstance ).\
                               filter( db.JobStatus.id.in_( [ TD.databaseIDs['pending'],TD.databaseIDs['running'] ] ) ).\
                               group_by( db.Host.full_name ).all() )

                # stopp running jobs


                # remove waiting
                loggerWrapper.write( "remove waiting jobs of user {u}".format(u=user ) )
                numRemovedJobs = dbconnection.query( db.WaitingJob ).filter( db.WaitingJob.user==userInstance ).delete()
                loggerWrapper.write( "... {j} waiting jobs have been removed".format(j=numRemovedJobs ) )
                #dbconnection.delete( *wJobs )

                # and finished jobs
                fJobs = dbconnection.query( db.FinishedJob ).join( db.Job ).filter( db.Job.user==userInstance ).all()
                loggerWrapper.write( "remove {n} finished jobs of user {u}".format(n=len(fJobs), u=user ) )
                dbconnection.delete( *fJobs )
                loggerWrapper.write( "...done" )

                # remove all jobs of user
                jobs = dbconnection.query( db.Job ).filter( db.Job.user==userInstance )
                numJobs = jobs.count()
                
                loggerWrapper.write( "remove {j} jobs of user {u}".format(j=numJobs, u=user ) )
                map( dbconnection.delete, jobs )
                loggerWrapper.write( "...done" )

                # update number occupied slots of each host
                for h in dbconnection.query( db.HostSummary ):
                    h.number_occupied_slots -= counts.get( h.id, 0)

                dbconnection.commit()

                request.send( "removed {j} of user {u}.".format(j=numJobs,u=user ) )
            except:
                traceback.print_exc(file=sys.stderr)
                
                request.send( "nothing has been removed" )
            
        elif self.commands["RMWJOBS"].re.match( requestStr ):
            c = self.commands["RMWJOBS"]
            
            userName = c.re.match( requestStr ).groups()[0]
            
            # remove all waiting jobs
            jobs = dbconnection.query( db.WaitingJob ).\
                   join( db.User ).\
                   filter( db.User.name==userName ).all()

            loggerWrapper.write( "remove {n} entries of user {u} from waiting_job table ".format(n=len(jobs), u=userName ) )

            dbconnection.delete( *jobs )
            dbconnection.commit()
            
            # remove jobs from database
            jobs = dbconnection.query( db.Job ).\
                   join( db.User ).\
                   join( db.JobDetails ).\
                   filter( and_(db.User.name==userName, db.JobDetails.job_status_id==TD.databaseIDs['waiting'] ) ).all()
            
            loggerWrapper.write( "remove {n} entries of user {u} from job table ".format(n=len(jobs), u=userName ) )
            
            dbconnection.delete( *jobs )
            dbconnection.commit()
            
            request.send( "done" )
        else:
            request.send("What do you want?")

        return

