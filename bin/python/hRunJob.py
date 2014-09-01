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
from pprint import pprint

homedir = os.environ['HOME']
user = pwd.getpwuid(os.getuid())[0]

# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../..') )

binPath    = '%s/bin' % tmpath
serverPath = '%s/UserServer' % tmpath	# for TMS
libPath  = '%s/lib' % tmpath		# for hSocket

sys.path.insert(0,libPath)

# make sure that libraries can be found, i.e., set PYTHONPATH appropriately
from hSocket import hSocket
from hServerProxy import hServerProxy
from hTaskDispatcherInfo import hTaskDispatcherInfo
from hTaskManagerServerInfo import hTaskManagerServerInfo

# get stored host and port from taskdispatcher
tmsInfo = hTaskManagerServerInfo()

tmsHost = tmsInfo.get('host', None)
tmsPort = tmsInfo.get('port', None)
useSSLConnection = tmsInfo.get('sslconnection', False)


if __name__ == '__main__':
    loginShell = os.environ['SHELL'].split('/')[-1]

    textWidth = 80
    parser = argparse.ArgumentParser(
        prog="hRunJob",
        usage = "usage: %(prog)s [-h --help] [options] COMMAND",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='\n'.join( textwrap.wrap("Run job in the cluster. Send the COMMAND to your TaskManagerServer", width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("  host: {}".format(tmsHost), width=textWidth)+
                               textwrap.wrap("  port: {}".format(tmsPort), width=textWidth)+
                               ['\n'] +
                               textwrap.wrap("if it is running or start a new one and send then the COMMAND to it.")
                               ),
        epilog='Written by Hendrik.')
    
    parser.add_argument('command',
                        nargs = '*',
                        metavar = 'COMMAND',
                        help = "Command which will be sent to the server." )    
    parser.add_argument("-E", "--excludeHosts",
                        metavar = "HOST[,HOST,...]",
                        dest = "excludeHosts",
                        default = "",
                        help = "Exclude computers from cluster for calculating given job. Consider option -H for a cluster overview." )
    parser.add_argument("-f", "--jobsFile",
                        metavar = "FilE",
                        default = "",
                        dest = "jobsFile",
                        help = "File in which in each line a job and respective additional info are given. Each line is tab-delimited with the fields group, info text, command, log file, stdout file, stderr file. Leave empty if respective field is not specified, but leave preceding tab. If field 'group' is not given in file the respective command line argument (if specified) is taken. if field info text, log file, stdout file, stderr file is not given in file the respective command line argument (if specified) is taken with an appended running number.")
    parser.add_argument("-g", "--group",
                        dest="group",
                        default="",
                        help="Assign a group given as a string to this job in order to refer to it later.")
    parser.add_argument("-i", "--info-text",
                        dest="infoText",
                        default="",
                        help="Info text about current job.")
    parser.add_argument("-l", "--logfile",
                        metavar="FILE",
                        dest="logfile",
                        default="",
                        help="Write log messages in FILE.")
    parser.add_argument("-o", "--stdout_file",
                        metavar="FILE",
                        dest="stdout",
                        default="",
                        help="Write output of command in FILE.")
    parser.add_argument("-O", "--stderr_file",
                       metavar="FILE",
                       dest="stderr",
                       default="",
                       help="Write error output of command in FILE.")
    parser.add_argument("-p", "--priority",
                       dest = "priority",
                       default = "0",
                       help = "Set priority of job. 1. Only two priorities are supported: 0 for no priority and non-zero for highest priority.")
    parser.add_argument("-q", "--quiet",
                       action="store_true",
                       dest="quiet",
                       default=False,
                       help="Do not print any status messages to stdout.")
    parser.add_argument("-s", "--status",
                       action="store_true",
                       dest="showStatus",
                       default=False,
                       help="Show information about taskmanager server and taskdispatcher.")
    parser.add_argument("-S", "--shell",
                       dest = "shell",
                       default = loginShell,
                       choices = ['tcsh','bash'],
                       help = "Define execution shell (tcsh or bash). Default: {shell}.".format(shell=loginShell))
    parser.add_argument("-v", "--verbose",
                       action = "store_true",
                       dest = "verboseMode",
                       default = False,
                       help = "Print additional information to stdout.")
    
    args = parser.parse_args()

    certfile = "%s/.taskmanager/%s.crt" % (homedir,user)
    keyfile = "%s/.taskmanager/%s.key" % (homedir,user)
    ca_certs = "%s/.taskmanager/ca_certs.%s.crt" % (homedir,user)


    ####################################
    # send requests to TMS
    
    command = join(args.command,' ')

    if args.showStatus:
        print "%25s: %s" % ("Taskmanager path",tmpath)
        print "%25s: %s" % ("Shell",args.shell)
        print


    TMS = hServerProxy( user = user,
                        serverType = 'TMS',
                        sslConnection = useSSLConnection,
                        keyfile = keyfile,
                        certfile = certfile,
                        ca_certs = ca_certs,
                        verboseMode = args.verboseMode )
    
    TMS.run()
    
    if not TMS.running:
        sys.stderr.write("Could not start a TMS!\n")
        sys.exit(-1)

    setPersistent = False
    
    # assemble requests
    if args.showStatus:
        jobs = ['gettmsinfo','gettdstatus']
        TMS.sendAndRecv("setpersistent")
        setPersistent = True
    #elif options.killTMS:
    #    jobs = ['shutdown']
    #elif options.jobID:
    #    jobs = ['lsjobinfo:%s' % options.jobID]
    #elif options.matchString:
    #    jobs = ['getmatchingjobs:%s' % options.matchString]
    #elif options.matchStringForKill:
    #    mStringSplit = options.matchStringForKill.split(":")
    #    mString  = mStringSplit[0]
    #    userName = ""
    #    if len(mStringSplit)==2:
    #        userName = mStringSplit[1]
    #    jobs = ['killmatchingjobs:%s:%s' % (mString,userName)]
    #elif options.jobIDForKill:
    #    jobs = ['killjob:%s' % options.jobIDForKill]
    #elif options.clusterOverview:
    #    jobs = ['lsactivecluster']
    else:
        if args.jobsFile:
            # send several jobs at once
            jsonObj = { 'shell': args.shell,
                        'priority': args.priority,
                        'excludedHosts': args.excludeHosts.split(','),
                        'user': user,
                        'jobs': [] }

            with open(args.jobsFile,'r') as f:
                for idx,line in enumerate(f):
                    try:
                        # neglect those lines with a leading '#'
                        if line[0] == '#': continue
                        
                        group, infoText, command, logFile, stdoutFile, stderrFile = line.strip('\n').split( '\t' )
                        
                    except:
                        # read next row
                        continue

                    jsonObj['jobs'].append( {'command': command,
                                             'infoText': infoText.format(idx=idx) if infoText else args.infoText+" "+str(idx) if args.infoText else '',
                                             'group': group if group else args.group,
                                             'stdout': stdoutFile.format(idx=idx) if stdoutFile else args.stdout+"_"+str(idx) if args.stdout else '',
                                             'stderr': stderrFile.format(idx=idx) if stderrFile else args.stderr+"_"+str(idx) if args.stderr else '',
                                             'logfile': logFile.format(idx=idx) if logFile else args.logfile+"_"+str(idx) if args.logfile else '' } )
                jsonObj = json.dumps(jsonObj)
                
            jobs = ['addjobs:%s' % jsonObj]
        else:
            # send a single job
            jsonObj = {'command': command,
                       'infoText': args.infoText,
                       'group': args.group,
                       'stdout': args.stdout,
                       'stderr': args.stderr,
                       'logfile': args.logfile,
                       'shell': args.shell,
                       'priority': args.priority,
                       'excludedHosts': args.excludeHosts.split(','),
                       'user': user }

            jsonObj = json.dumps(jsonObj)

            jobs = ['addjob:%s' % jsonObj]


    pprint( jobs )
    asdkfj
    #send commands to TMS
    try:
        for i,job in enumerate(jobs):
            
            TMS.send(job)
            recv = TMS.recv()

            if recv and args.showStatus:
                ## show status of TMS and TD
                
                recvSplit = recv.split(':')
                
                if i==0:	# gettmsinfo TMS info
                    print "%25s: %s" % ("TMS host",TMS.host)
                    print "%25s: %s" % ("TMS port",TMS.port)
                    if TMS.started:
                        print "%25s: (new) %s" % ("running since",recvSplit[0])
                    else:
                        print "%25s: %s" % ("running since",recvSplit[0])
                    print "%25s: %s" % ("running jobs in TMS",recvSplit[1])
                    print "%25s: %s" % ("waiting jobs in TMS",recvSplit[2])
                    print
                elif i==1:	# gettdinfo TD info
                    print recvSplit
                    #print "%25s: %s" % ("TaskDispatcher host", recvSplit[0])
                    #print "%25s: %s" % ("TaskDispatcher port", recvSplit[1])
                elif i==2:	# gettdstatus TD status
                    print recvSplit
                    ##print "%25s: %s" % ("connection status",recvSplit[0])
                    #if len(recvSplit)==6:
                    #    print "%25s: %s" % ("running since",recvSplit[0])
                    #    print "%25s: %s" % ("total CPUs", recvSplit[2])
                    #    print "%25s: %s" % ("free CPUs", recvSplit[1])
                    #    print "%25s: %s" % ("running jobs",recvSplit[3])
                    #    print "%25s: %s" % ("waiting jobs",recvSplit[4])
                    #    print "%25s: %s" % ("finished jobs",recvSplit[5])
                    #else:
                    #    print "%25s: %s" % ("connection status","failed!")
                        
                    
            #elif args.killTMS:
            #    print "TaskManagerServer %s:%s was killed" % (TMS.host,TMS.port)
                
            else: 
                ## usual request to TMS
                if not args.quiet:
                    print recv
                
                pass

        if setPersistent:
            TMS.sendAndRecv("unsetpersistent")
            
        TMS.close()
        
    except socket.error,msg: 
        print "ERROR while connecting to TMS:",msg

    sys.exit(0)
