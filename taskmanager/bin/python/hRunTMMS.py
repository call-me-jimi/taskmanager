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

from hTaskManagerServerInfo import hTaskManagerServerInfo
from TMMS import TaskManagerMenialServer, TaskManagerMenialServerHandler, TaskManagerMenialServerProcessor
from hTMUtils import getDefaultTMMSPort

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
    defaultTMMSHost = os.uname()[1]

    # default port number is constructed from the user name
    defaultTMMSPort = getDefaultTMMSPort( user )

    # get stored host and port from taskdispatcher
    tmsInfo = hTaskManagerServerInfo()

    textWidth = 80
    parser = argparse.ArgumentParser(
        prog="hRunTMMS",
        usage="%(prog)s [-h --help] [Options]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='\n'.join( textwrap.wrap("Run a TaskManagerMenialServer which starts and stops jobs on host. Server is started (by default) on", width=textWidth) +
                               ['\n'] + 
                               [ '   Host: {host}'.format(host=defaultTMMSHost) ] +
                               [ '   Port: {port}'.format(port=defaultTMMSPort) ] +
                               ['\n'] + 
                               textwrap.wrap("either in background or in foreground with option -d")
                               ),
        epilog='Written by Hendrik.')
    
    parser.add_argument("-d", "--nonDaemonMode",
                        action="store_false",
                        dest="daemonMode",
                        default=True,
                        help="Start TMMS in foreground (not as daemon).")
    parser.add_argument("-v", "--verbose",
                        action = "store_true",
                        dest = "verboseMode",
                        default = False,
                        help = "Print additional information to stdout.")
    parser.add_argument("-p", "--port",
                        type = int,
                        dest = 'tmmsPort',
                        default = defaultTMMSPort,
                        help = "Port on which the server will start. Default: {port}".format(port=defaultTMMSPort)
                        )
                      
    args = parser.parse_args()

    certfile = "%s/.taskmanager/%s.crt" % (homedir,user)
    keyfile = "%s/.taskmanager/%s.key" % (homedir,user)
    ca_certs = "%s/.taskmanager/ca_certs.%s.crt" % (homedir,user)

    ####################################
    # start TMMS
    TMMS = None
    try:
        TMMS=TaskManagerMenialServer( port=args.tmmsPort,
                                      tmsHost=tmsInfo.get('host'),
                                      tmsPort=tmsInfo.get('port'),
                                      handler=TaskManagerMenialServerHandler,
                                      processor=TaskManagerMenialServerProcessor(),
                                      verboseMode=args.verboseMode,
                                      EOCString=EOCString,
                                      sslConnection=useSSLConnection,
                                      keyfile=keyfile,
                                      certfile=certfile,
                                      ca_certs=ca_certs,
                                      logFileTMMS=None )

        if args.daemonMode:
            if args.verboseMode:
                print "TMMS has been started as daemon on {host}:{port}".format(host=TMMS.host, port=TMMS.port)
            TMMS.start() # run as deamon
        else:
            TMMS.run() # run not as daemon

    except exceptions.KeyboardInterrupt:
        sys.stderr.write("Keyboard interrupt.\n")
        
        sys.stderr.write("TMMS shutdown.\n")
        
        sys.stderr.flush()
        sys.stdout.flush()
    except exceptions.SystemExit,msg:
        sys.stderr.write("System exit.\n")
        
        sys.stderr.write("TMMS shutdown.\n")
        sys.stderr.flush()
        sys.stdout.flush()
    except:
        tb = sys.exc_info()
        
        sys.stderr.write("TMMS Error: %s\n" % sys.exc_info()[1])
        sys.stderr.write("TRACEBACK:")
        traceback.print_exception(*tb,file=sys.stderr)
    finally:
        logging.shutdown()
        sys.exit(0)


