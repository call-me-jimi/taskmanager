#!/usr/bin/env python

import sys
import socket
import os
import pwd
from string import join, letters, digits
from random import choice
import argparse
import json
import time
import traceback
import exceptions
import subprocess
import re
import textwrap
import time
import logging
import ConfigParser

homedir = os.environ['HOME']
user = pwd.getpwuid(os.getuid())[0]

# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../..') )

binPath  = '%s/bin' % tmpath
etcPath  = '%s/etc' % tmpath
libPath  = '%s/lib' % tmpath		# for hSocket

sys.path.insert(0,libPath)

from hTaskDispatcherInfo import hTaskDispatcherInfo
from TMS import TaskManagerServer, TaskManagerServerHandler, TaskManagerServerProcessor
from hTMUtils import getDefaultTMSPort, cleanUp

class ValidateNonDaemonNotPersistentMode(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        #print '{n} -- {v} -- {o}'.format(n=namespace, v=value, o=option_string)
        
        ## set attribute self.dest, i.e., 'persistent'
        setattr(namespace, self.dest, False)

        ## set attribute 'daemonMode'
        setattr(namespace, 'daemonMode', False)
        
if __name__ == '__main__':
    # read default config file
    defaultConfigFileName = "%s/serversettings.cfg" % etcPath
    if os.path.exists( defaultConfigFileName ):
        defaultConfig = ConfigParser.ConfigParser()
        defaultConfig.read( defaultConfigFileName )
    
    useSSLConnection = defaultConfig.getboolean( 'CONNECTION', 'sslConnection' )
    EOCString = defaultConfig.get( 'CONNECTION', 'EOCString' )

    loginShell = os.environ['SHELL'].split('/')[-1]

    # define host and port
    defaultTMSHost = os.uname()[1]

    # default port number is constructed from the user name
    defaultTMSPort = getDefaultTMSPort( user )

    textWidth = 80
    parser = argparse.ArgumentParser(
        prog="hRunTMS",
        usage="%(prog)s [-h --help] [Options]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='\n'.join( textwrap.wrap("Run a TaskManagerServer either in background or with option -d or -D in foreground. Start server on", width=textWidth) +
                               ['\n'] + 
                               [ '   Host: {host}'.format(host=defaultTMSHost) ] +
                               [ '   Port: {port}'.format(port=defaultTMSPort) ]
                               ),
        epilog='Written by Hendrik.')
    
    parser.add_argument("-d", "--nonDaemonMode",
                        action="store_false",
                        dest="daemonMode",
                        default=True,
                        help="Start TMS in foreground (not as daemon).")
                        #help="Start TMS in foreground (not as daemon). TMS will be shut down after last job has been processed. Make sure that TMS is not running before.")
    parser.add_argument("-D", "--nonDaemonNotPersistentMode",
                        nargs=0,
                        action=ValidateNonDaemonNotPersistentMode,	# set as well daemonMode attribute
                        dest="persistent",
                        default=True,
                        help="Start TMS in foreground (not as daemon) as a non-persistent server. TMS will be shut down after last job has been processed.")
    #parser.add_argument("-t", "--startTMS",
    #                    action="store_true",
    #                    dest="startTMS",
    #                    default=False,
    #                    help="Start TMS explicitly and print TMS info messages to stdout forever. Make sure that TMS is not running.")
    parser.add_argument("-v", "--verbose",
                        action = "store_true",
                        dest = "verboseMode",
                        default = False,
                        help = "Print additional information to stdout.")
    parser.add_argument("-p", "--port",
                        type = int,
                        dest = 'tmsPort',
                        default = defaultTMSPort,
                        help = "Port on which the server will start. Default: {port}".format(port=defaultTMSPort)
                        )
                      
    args = parser.parse_args()

    certfile = "%s/.taskmanager/%s.crt" % (homedir,user)
    keyfile = "%s/.taskmanager/%s.key" % (homedir,user)
    ca_certs = "%s/.taskmanager/ca_certs.%s.crt" % (homedir,user)

    ####################################
    # start TMS
    TMS = None
    try:
        TMS=TaskManagerServer( port=args.tmsPort,
                               EOCString=EOCString,
                               sslConnection=useSSLConnection,
                               keyfile=keyfile,
                               certfile=certfile,
                               ca_certs=ca_certs,
                               handler=TaskManagerServerHandler,
                               processor=TaskManagerServerProcessor(),
                               verboseMode=args.verboseMode,
                               persistent=args.persistent,
                               logFileTMS=None )

        if args.daemonMode:
            if args.verboseMode:
                print "TMS has been started as daemon on {host}:{port}".format(host=TMS.host, port=TMS.port)
            TMS.start() # run as deamon
        else:
            TMS.run() # run not as daemon

        try:
            cleanUp(TMS)
        except:
            #sys.stderr.write("%s\n" % sys.exc_info())
            sys.stderr.write("Termination of all TMMS has failed!\n")
    except exceptions.KeyboardInterrupt:
        sys.stderr.write("Keyboard interrupt.\n")
        
        try:
            cleanUp(TMS)
        except:
            sys.stderr.write("Termination of all TMMS has failed!\n")

        sys.stderr.write("TMS shutdown.\n")
        
        sys.stderr.flush()
        sys.stdout.flush()
    #except socket.error,msg:
    #    try:
    #        terminateAllTMMS(TMS)
    #    except:
    #        sys.stderr.write("Termination of all TMMS has failed!\n")
    #
    #    sys.stderr.write("TMS (%s:%s) Socket Error: %s\n" % (host,port,msg))
    #    sys.stderr.write("TMS shut down.\n")
    except exceptions.SystemExit,msg:
        sys.stderr.write("System exit.\n")
        try:
            cleanUp(TMS)
        except:
            sys.stderr.write("Termination of all TMMS has failed!\n")

        sys.stderr.write("TMS shutdown.\n")
        sys.stderr.flush()
        sys.stdout.flush()
    except:
        tb = sys.exc_info()
        
        if TMS:
            try:
                cleanUp(TMS)
            except:
                sys.stderr.write("Termination of all TMMS has failed!\n")

        sys.stderr.write("TMS Error: %s\n" % sys.exc_info()[1])
        sys.stderr.write("TRACEBACK:")
        traceback.print_exception(*tb,file=sys.stderr)
    finally:
        logging.shutdown()
        sys.exit(0)




    #######################################
    #### start a TMS (if nesessary), connect to it and and listen to port
    ###if args.startTMS:
    ###    if args.verboseMode:
    ###        print "start TMS, connect to it and listen to port"
    ###        
    ###    TMS = RunTMS( sslConnection=sslConnection,
    ###                  keyfile = keyfile,
    ###                  certfile = certfile,
    ###                  ca_certs = ca_certs,
    ###                  logFile = options.logfileTMS,
    ###                  binPath = binPath,
    ###                  vMode = options.verboseMode)
    ###        
    ###    TMS.run()
    ###    
    ###    if TMS.running:
    ###        if options.verboseMode:
    ###            if TMS.started:
    ###                print "... TMS has been started"
    ###            else:
    ###                print "... TMS was running"
    ###
    ###
    ###        tmsConn = TMConnection(host=TMS.host,
    ###                               port=TMS.port,
    ###                               sslConnection=TMS.sslConnection,
    ###                               keyfile=TMS.keyfile,
    ###                               certfile=TMS.certfile,
    ###                               ca_certs=TMS.ca_certs,
    ###                               catchErrors=False,
    ###                               verboseMode=options.verboseMode)
    ###
    ###        command = "addInfoSocket"
    ###
    ###        #    clientSock = hSocket(host=TMS.host,
    ###        #                         port=TMS.port,
    ###        #                         sslConnection=TMS.sslConnection,
    ###        #                         certfile=TMS.certfile,
    ###        #                         keyfile=TMS.keyfile,
    ###        #                         ca_certs=TMS.ca_certs,
    ###        #                         catchErrors=False)
    ###        tmsConn.send(command)
    ###
    ###        #    clientSock.send(command)
    ###        #    clientSock.send("@@@@")
    ###        
    ###        # loop until break requested by user
    ###        if options.verboseMode:
    ###            print "... and listen"
    ###
    ###        while 1:
    ###            output = tmsConn.clientSock.recv(65536).strip('\n')
    ###            if output:
    ###                print output
    ###            else:
    ###                break
    ###        print "TMS was shut down."
    ###        
    ###        tmsConn.close()
    ###        
    ###    sys.exit(0)
    ##################### END permanent listening on port


        
    ######################################
    ### send requests to TMS
    ##
    ### showStatus does not require an argument
    ##if not options.showStatus and not options.clusterOverview and not options.killTMS and not options.matchString and not options.matchStringForKill and not options.jobID  and not options.jobIDForKill and len(args) == 0:
    ##    parser.error("Command is a required argument needed to start a calculation.\nTry `%s -h or --help' for more information." % sys.argv[0].split('/')[-1])
    ##    
    ##
    ##if not options.showStatus and not options.killTMS and not options.jobID:
    ##    command = join(args,' ')
    ##
    ##if options.showStatus:
    ##    print "%25s: %s" % ("Taskmanager path",tmpath)
    ##    print "%25s: %s" % ("Shell",options.shell)
    ##    print
    ##
    ##
    ##TMS = RunTMS( sslConnection = sslConnection,
    ##              keyfile = keyfile,
    ##              certfile = certfile,
    ##              ca_certs = ca_certs,
    ##              logFile = options.logfileTMS,
    ##              binPath = binPath,
    ##              vMode = options.verboseMode)
    ##
    ##TMS.run()
    ##
    ##if not TMS.running:
    ##    sys.stderr.write("Could not start a TMS!\n")
    ##    sys.exit(-1)
    ##
    ##tmsConn = TMConnection(host=TMS.host,
    ##                       port=TMS.port,
    ##                       sslConnection=TMS.sslConnection,
    ##                       keyfile=TMS.keyfile,
    ##                       certfile=TMS.certfile,
    ##                       ca_certs=TMS.ca_certs,
    ##                       catchErrors=False,
    ##                       verboseMode=options.verboseMode)
    ##
    ### assemble requests
    ##setPersistent = False
    ##if options.showStatus:
    ##    jobs = ['gettmsinfo','gettdinfo','gettdstatus']
    ##    tmsConn.sendAndRecv("setpersistent")
    ##    setPersistent = True
    ##elif options.killTMS:
    ##    jobs = ['shutdown']
    ##elif options.jobID:
    ##    jobs = ['lsjobinfo:%s' % options.jobID]
    ##elif options.matchString:
    ##    jobs = ['getmatchingjobs:%s' % options.matchString]
    ##elif options.matchStringForKill:
    ##    mStringSplit = options.matchStringForKill.split(":")
    ##    mString  = mStringSplit[0]
    ##    userName = ""
    ##    if len(mStringSplit)==2:
    ##        userName = mStringSplit[1]
    ##    jobs = ['killmatchingjobs:%s:%s' % (mString,userName)]
    ##elif options.jobIDForKill:
    ##    jobs = ['killjob:%s' % options.jobIDForKill]
    ##elif options.clusterOverview:
    ##    jobs = ['lsactivecluster']
    ##else:
    ##    if ":" in command+options.info_text:
    ##        sys.stderr.write("Command or info contains ':'!\nThat is not allowed.\nQuit.\n")
    ##        sys.exit(-1)
    ##        
    ##    
    ##    #jobs = ['addjob:%s:%s:%s:%s:%s' %(options.info_text,
    ##    #                                   command,
    ##    #                                   options.logfile,
    ##    #                                   options.shell,
    ##    #                                   options.priority)]
    ##    jsonObj = {'jobInfo': options.info_text,
    ##               'command': command,
    ##               'logFile': options.logfile,
    ##               'shell': options.shell,
    ##               'priority': options.priority,
    ##               'excludedHosts': options.excludeHosts.split(',')}
    ##
    ##    jsonObj = json.dumps(jsonObj)
    ##
    ##    jobs = ['addjobJSON:%s' % jsonObj]
    ##
    ##
    ### prepend additional td info ( TD:<host>:port )
    ##if options.TD:
    ##    td = "TD:%s:%s" % (options.TD)
    ##    jobs = map(lambda s: td+s,jobs)
    ##
    ###send commands to TMS
    ##try:
    ##    for i,job in enumerate(jobs):
    ##        
    ##        tmsConn.send(job)
    ##        recv = tmsConn.recv()
    ##
    ##        if recv and options.showStatus:
    ##            # show status of TMS and TD
    ##            recvSplit = recv.split(':')
    ##            
    ##            if i==0:	# gettmsinfo TMS info
    ##                print "%25s: %s" % ("TMS host",TMS.host)
    ##                print "%25s: %s" % ("TMS port",TMS.port)
    ##                if TMS.started:
    ##                    print "%25s: (new) %s" % ("running since",recvSplit[0])
    ##                else:
    ##                    print "%25s: %s" % ("running since",recvSplit[0])
    ##                print "%25s: %s" % ("running jobs in TMS",recvSplit[1])
    ##                print "%25s: %s" % ("waiting jobs in TMS",recvSplit[2])
    ##                print
    ##            elif i==1:	# gettdinfo TD info
    ##                print "%25s: %s" % ("TaskDispatcher host", recvSplit[0])
    ##                print "%25s: %s" % ("TaskDispatcher port", recvSplit[1])
    ##            elif i==2:	# gettdstatus TD status
    ##                #print "%25s: %s" % ("connection status",recvSplit[0])
    ##                if len(recvSplit)==6:
    ##                    print "%25s: %s" % ("running since",recvSplit[0])
    ##                    print "%25s: %s" % ("total CPUs", recvSplit[2])
    ##                    print "%25s: %s" % ("free CPUs", recvSplit[1])
    ##                    print "%25s: %s" % ("running jobs",recvSplit[3])
    ##                    print "%25s: %s" % ("waiting jobs",recvSplit[4])
    ##                    print "%25s: %s" % ("finished jobs",recvSplit[5])
    ##                else:
    ##                    print "%25s: %s" % ("connection status","failed!")
    ##                    
    ##                
    ##        elif options.killTMS:
    ##            print "TaskManagerServer %s:%s was killed" % (TMS.host,TMS.port)
    ##            
    ##        else: # usual request to TMS
    ##            print recv
    ##            pass
    ##
    ##    if setPersistent:
    ##        tmsConn.sendAndRecv("unsetpersistent")
    ##        
    ##    tmsConn.close()
    ##    
    ##except socket.error,msg: 
    ##    print "ERROR while connecting to TMS:",msg
    ##
    ##sys.exit(0)
