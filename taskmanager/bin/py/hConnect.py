#!/usr/bin/env python

import socket
import getopt
import sys
from time import sleep
import re
import os
import pwd
import traceback
import argparse
import textwrap
import collections

# logging
import sys
import logging
logger = logging.getLogger('hConnect')
logger.propagate = False
logger.setLevel(logging.ERROR)			# logger level. can be changed with command line option -v

formatter = logging.Formatter('[%(asctime)-15s] %(message)s')

# create console handler and configure
consoleLog = logging.StreamHandler(sys.stdout)
consoleLog.setLevel(logging.INFO)		# handler level. 
consoleLog.setFormatter(formatter)

# add handler to logger
logger.addHandler(consoleLog)

homedir = os.environ['HOME']
user=pwd.getpwuid(os.getuid())[0]

# get path to taskmanager. it is assumed that this file is in the bin/python directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/../..') )

varpath  = '%s/var' % tmpath	# for host:port of taskdispatcher
libpath  = '%s/lib' % tmpath	# for hSocket

sys.path.insert(0,libpath)

from hSocket import hSocket
from hServerProxy import hServerProxy
from hTaskDispatcherInfo import hTaskDispatcherInfo
from hTaskManagerServerInfo import hTaskManagerServerInfo

# get stored host and port from taskdispatcher
tdInfo = hTaskDispatcherInfo()

tdHost = tdInfo.get('host', None)
tdPort = tdInfo.get('port', None)
useSSLConnection = tdInfo.get('sslconnection', False)

# get stored host and port from tms
tmsInfo = hTaskManagerServerInfo()

tmsHost = tmsInfo.get('host', None)
tmsPort = tmsInfo.get('port', None)


# create tuple like object with field host and port
HostAndPort = collections.namedtuple( 'HostAndPort', ['host', 'port'])

class ValidateHostAndPort(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # print '{n} -- {v} -- {o}'.format(n=namespace, v=values, o=option_string)
        
        host, port = values

        # port number should be an int
        try:
            port = int(port)
        except:
            raise argparse.ArgumentError(self, 'invalid port number {p!r}'.format(p=port))

        # set attribute self.dest with field host and port
        setattr(namespace, self.dest, HostAndPort(host, port))
        # add another attribute
        setattr(namespace, "useHostAndPort", True)

        
class ValidateBool(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        #print '{n} -- {v} -- {o}'.format(n=namespace, v=value, o=option_string)

        value = True if value=='True' else False

        # set attribute self.dest
        setattr(namespace, self.dest, value)

class ValidateVerboseMode(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        #print '{n} -- {v} -- {o}'.format(n=namespace, v=value, o=option_string)

        # set level of logger to INFO
        logger.setLevel( logging.INFO )
        
        # set attribute self.dest
        setattr(namespace, self.dest, True)
        
if __name__ == '__main__':
    # read default configurations from file
    
    defaultEOCString = "@@@@"	# end of submission string

    textWidth = 80
    parser = argparse.ArgumentParser(
        prog="hConnect",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage="%(prog)s [-h --help] [options] COMMAND",
        description='\n'.join( textwrap.wrap("Connect to your TaskManagerServer", width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("  host: {}".format(tmsHost), width=textWidth) +
                               textwrap.wrap("  port: {}".format(tmsPort), width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("send the COMMAND to it and print response to stdout. If the TMS is not running, a TMS will be started. If you want to connect to the TaskDispatcher", width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("  host: {}".format(tdHost), width=textWidth) +
                               textwrap.wrap("  port: {}".format(tdPort), width=textWidth) +
                               ['\n'] +
                               textwrap.wrap("use option -T. If you want to connect to another server, specify host and port with option -S.", width=textWidth)
                               ),
        epilog='Written by Hendrik.')
    parser.add_argument('command',
                        metavar = 'COMMAND',
                        help = "Command which will be sent to the server."
                        )
    
    parser.add_argument('-e', '--do_not_use_eocstring',
                        dest = 'useEOCString',
                        action = 'store_false',
                        default = True,
                        help = 'Do not use EOCString. The default EOCString is "{eocs}". This can be changed by option -E.'.format(eocs=defaultEOCString)
                       )
    
    parser.add_argument('-E', '--eocstring', 
                        dest = "EOCString", 
                        default = defaultEOCString,
                        help = 'Use this EndOfCommunication string if option -e is given.'
                        )
    
    parser.add_argument('-s', '--use_ssl_connection',
                        dest = 'useSSLConnection',
                        choices = ('True','False'),
                        action = ValidateBool,
                        default = useSSLConnection,
                        help = 'Use secure socket connection. Default: {v}'.format(v=str(useSSLConnection))
                        )
    
    parser.add_argument('-S', '--server_settings',
                        nargs = 2,
                        metavar = ('HOST','PORT'),
                        action = ValidateHostAndPort,
                        dest = 'serverSettings',
                        default = HostAndPort(tmsHost,tmsPort),
                        help = 'Connect to server HOST:PORT. Default {h}:{p}'.format(h=tmsHost, p=tmsPort)
                        )

    parser.add_argument('-T', '--connect_to_td',
                        dest = 'connectToTD',
                        action = 'store_true',
                        default = False,
                        help = 'Connect to TaskDispatcher {host}:{port}.'.format(host=tdHost,port=tdPort)
                        )
    
    parser.add_argument('-v', '--verbose_mode',
                        nargs = 0,
                        dest = 'verboseMode',
                        action = ValidateVerboseMode,
                        default = False,
                        help = 'Verbose mode'
                        )
    
    args = parser.parse_args()

    # if not using EndOfCommunication string then set it to empty string
    if not args.useEOCString:
        args.EOCString = None

    # here the the certificates should be read
    keyfile = None
    certfile = None
    ca_certs = None

    # set server host and port to which we try to connect
    if hasattr(args,'connectToTD') and args.connectToTD:
        host = tdHost
        port = tdPort
    else:
        host = args.serverSettings.host
        port = args.serverSettings.port

    try:
        if not args.connectToTD and not ( hasattr(args,'useHostAndPort') and args.useHostAndPort ):
            client = hServerProxy( user = user,
                                   serverType = 'TMS',
                                   verboseMode = args.verboseMode )

            client.run()

            if not client.running:
                sys.stderr.write("Could not start a TMS!\n")
                sys.exit(-1)

        else:    
            # create socket
            client = hSocket( sslConnection=args.useSSLConnection,
                              keyfile = keyfile,
                              certfile = certfile,
                              ca_certs = ca_certs,
                              EOCString = args.EOCString,
                              catchErrors = False)

            client.initSocket( host, port )

        logger.info( "Connection to {host}:{port}".format( host=host, port=port ) )

        client.send(args.command)

        logger.info( "Command: {com}".format(com=args.command ))

        receivedStr = client.recv()

        logger.info( "Received string:")
        
        sys.stdout.write(receivedStr)
        
        if receivedStr:
            sys.stdout.write("\n")

        logger.info("Length of received string: {l}".format(l=len(receivedStr)))
            
        if args.connectToTD or ( hasattr(args,'useHostAndPort') and args.useHostAndPort ):
            client.close()

    except socket.error,msg:
        print "ERROR while connecting to %s:%s with error %s" % (host, port, msg)
        if args.verboseMode:
            print "TRACBACK:"
            traceback.print_exc(file=sys.stdout)

