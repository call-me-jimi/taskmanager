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
    parser.add_argument("-c", "--slots",
                        metavar = "SLOTS",
                        dest = "slots",
                        type = int,
                        default = 1,
                        help = "Number cores on a host which will be used for this job." )
    parser.add_argument("-E", "--excludeHosts",
                        metavar = "HOST[,HOST,...]",
                        dest = "excludedHosts",
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
    
    args.command = join(args.command,' ')

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

    TMS.sendAndRecv("setpersistent")

    requests = []
    
    # assemble requests
    if args.showStatus:
        requests = ['printserverinfo']
        
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
            jsonObj = {  }

            with open(args.jobsFile,'r') as f:
                jobs = []
                
                # known fields
                requiredFields = [ "command", "slots", "group", "infoText", "logfile", "stdout", "stderr", "shell", "priority", "excludedHosts" ]
                # construct regular expressions
                reRequiredFields = { field: re.compile( "^{field}::(.*)$".format(field=field) ) for field in requiredFields }

                # iterate over all lines in file. add only 100 jobs at once
                for idx,line in enumerate(f):
                    line = line.strip('\n')
                    
                    jobDetails = {}
                    
                    try:
                        # neglect lines with a leading '#' and empty lines
                        if line[0] == '#' or line=="": continue
                        
                        # parse line
                        lineSplitted = line.split( '\t' )

                        jobDetails = {}
                        #iterate over all known fields and add value to jobDetails
                        # not really good style here!!!
                        for field,reField in reRequiredFields.iteritems():
                            found = False
                            # iterate over all entries in line
                            for entry in lineSplitted:
                                m = reField.match( entry )
                                if m:
                                    d = m.groups()[0]
                                    
                                    jobDetails[ field ] = d.format( idx=idx )
                                    found = True
                                    break

                            if not found:
                                # field value is not specified for current job
                                # use values given as command line argument (given in args)
                                jobDetails[ field ] = getattr(args, field)

                        # at least command has to be specified
                        if jobDetails['command']:
                            jobs.append( jobDetails )
                    except:
                        # read next row
                        #traceback.print_exc(file=sys.stdout)
                        continue


                    # add maximal 100 jobs to a single request
                    if idx>0 and idx % 100 == 1:
                        # add jobs
                        jsonObj = json.dumps( jobs )
                        requests.append( 'addjobs:%s' % jsonObj )
                        jobs = []

                if jobs:
                    print "add last jobs"
                    jsonObj = json.dumps( jobs )
                    requests.append( 'addjobs:%s' % jsonObj )

        else:
            # send a single job
            jsonObj = {'command': args.command,
                       'slots': args.slots,
                       'infoText': args.infoText,
                       'group': args.group,
                       'stdout': args.stdout,
                       'stderr': args.stderr,
                       'logfile': args.logfile,
                       'shell': args.shell,
                       'priority': args.priority,
                       'excludedHosts': args.excludedHosts }

            jsonObj = json.dumps(jsonObj)

            requests.append('addjob:%s' % jsonObj)

    #send commands to TMS
    try:
        for i,job in enumerate(requests):
            try:
                TMS.send(job)
                recv = TMS.recv()
                
                # print response
                if not args.quiet:
                    print recv
            except:
                traceback.print_exc(file=sys.stdout)

        TMS.sendAndRecv("unsetpersistent")
        TMS.close()
        
    except socket.error,msg: 
        print "ERROR while connecting to TMS:",msg

    sys.exit(0)
