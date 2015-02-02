#!/usr/bin/env python

import sys
import argparse
import os
import textwrap
import ConfigParser
from datetime import datetime
import socket
import traceback

# get path to taskmanager. it is assumed that this script is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../..' ) )

#serverpath = '%s/server' % tmpath	# for InfoServer
etcpath    = '%s/etc'    % tmpath	# for configuration files
libpath  = '%s/lib' % tmpath		# for hSocket
#varpath  = '%s/var' % tmpath		# for info file

# include lib path of the TaskManager package to sys.path for loading TaskManager packages
sys.path.insert(0,libpath)

from TaskDispatcher import TaskDispatcher,TaskDispatcherRequestHandler,TaskDispatcherRequestProcessor


#####################################
if __name__ == '__main__':
    # read default config file
    defaultConfigFileName = "%s/serversettings.cfg" % etcpath
    if os.path.exists( defaultConfigFileName ):
        defaultConfig = ConfigParser.ConfigParser()
        defaultConfig.read( defaultConfigFileName )
    
    defaultTDsslConnection = defaultConfig.getboolean( 'CONNECTION', 'sslConnection' )
    defaultTDEOCString = defaultConfig.get( 'CONNECTION', 'EOCString' )
    
    # read taskdispatcher config file
    tdConfigFileName = "%s/taskdispatcher.cfg" % etcpath
    
    if os.path.exists( tdConfigFileName ):
        tdConfig = ConfigParser.ConfigParser()
        tdConfig.read( tdConfigFileName )

    defaultTDHost = os.uname()[1]
    defaultTDPort = tdConfig.getint( 'ADDRESS', 'TaskDispatcherPort' )
        
    defaultErrorLogFile = '/tmp/Taskdispatcher.{port}.err'

    textWidth = 80
    parser = argparse.ArgumentParser(
        prog="hRunTaskDispatcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='\n'.join( textwrap.wrap("Run the the TaskDispatcher on", width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("  host: {}".format(defaultTDHost), width=textWidth)+
                               textwrap.wrap("  port: {}".format(defaultTDPort), width=textWidth)+
                               ['\n']+
                               textwrap.wrap("as a server for a task management system.", width=textWidth) +
                               ['\n']+
                               textwrap.wrap("By default an error logfile '{fileName}' is created. This can be changed with the option -e.".format(fileName=defaultErrorLogFile.format(port=defaultTDPort) ), width=textWidth)
                               ),
        epilog='Written by Hendrik.')
    
    parser.add_argument('-p', '--port',
                        metavar = 'PORT',
                        dest = 'tdPort',
                        default = defaultTDPort,
                        type = int,
                        help = 'Start TaskDispatcher on PORT. Default: {port}.'.format(port=defaultTDPort)
                        )
    parser.add_argument('-q', '--quiet', 
                        dest="noOutput", 
                        action='store_true',
                        default=False,
                        help = 'Suppress most of the outputs.')
    parser.add_argument('-e', '--error-log-file',
                        metavar = 'FILE',
                        dest = 'errorLogFileName',
                        default = defaultErrorLogFile,
                        help = 'Write errors (exceptions) into FILE. Default: {fileName}.'.format(fileName=defaultErrorLogFile.format(port=defaultTDPort)  )
                        )
    parser.add_argument('-P', '--path',
                        metavar = 'PATH',
                        dest = 'packagedir',
                        help = 'Specify a different path to the TaskManager package. Default: {path}'.format( path=tmpath )
                        )
    
    args = parser.parse_args()
    

    # try to open logfile
    try:
        # add port to logfile name
        logfile = args.errorLogFileName.format(port=args.tdPort)

        if os.path.exists(logfile):
            # append to existing logfile
            logfileTDErrors = open(logfile, 'a')
        else:
            # create new logfile
            logfileTDErrors = open(logfile, 'w')
    
        logfileTDErrors.write('----------------------\n')
        logfileTDErrors.write("[%s]\n" % str(datetime.now()))
        logfileTDErrors.write('----------------------\n')
        logfileTDErrors.flush()
    
    except IOError,msg:
        traceback.print_exc(file=sys.stderr)
        sys.exit(-1)


    # run TaskDipatcher
    try:
        TD = TaskDispatcher( args.tdPort,
                             sslConnection=defaultTDsslConnection,
                             EOCString=defaultTDEOCString,
                             logfileTDErrors=logfileTDErrors,
                             handler=TaskDispatcherRequestHandler,
                             processor=TaskDispatcherRequestProcessor(),
                             message='Welcome to the TaskDispatcher.' )

        
        TD.run() # run not as daemen; for debugging
    except socket.error,msg:
        sys.stderr.write("\n")
        sys.stderr.write("TaskDispatcher Socket Error: %s\n" % msg.message)
        traceback.print_exc(file=sys.stdout)
    except SystemExit,msg:
        sys.stderr.write("\n")
        if msg.code == 0:
            sys.stderr.write("TaskDispatcher Exit\n")
        else:
            sys.stderr.write("TaskDispatcher System Error: %s\n" % msg.message)
    except:
        sys.stderr.write("\n")
        sys.stderr.write("TaskDispatcher Error: %s\n" % sys.exc_info()[0])
        traceback.print_exc(file=sys.stdout)
        #raise
        
    sys.stdout.flush()
    sys.stderr.flush()

