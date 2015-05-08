import socket
import ssl
import re
from datetime import datetime,timedelta,date
import traceback
import sys

class hSocket( object ):
    """ class for socket communication"""
    def __init__(self,
                 sock=None,
                 host=None,
                 port=None,
                 sslConnection=False,
                 serverSideSSLConn=False,
                 certfile=None,
                 keyfile=None,
                 ca_certs=None,
                 logFileIn=None,
                 logFileOut=None,
                 catchErrors=True,
                 timeout=None,
                 EOCString=None):
        self.socket = sock
        self.logFileIn = logFileIn
        self.logFileOut = logFileOut
        self.host = host
        self.port = port
        self.catchErrors = catchErrors
        self.sentStr = None
        self.receivedStr = None
        self.timeout = timeout
        self.connectionError = False
        self.EOCString = EOCString	# end of communication string

        self.sslConnection=sslConnection
        self.serverSideSSLConn=serverSideSSLConn
        self.certfile=certfile
        self.keyfile=keyfile
        self.ca_certs=ca_certs

        if self.host and self.port:
            self.initSocket(self.host,self.port)
            if self.logFileIn:
                h,p = self.socket.getpeername()
                #self.logFileIn.write("[%s] [%s:%s] open socket\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
            
        if self.socket:
            try:
                self.host,self.port = self.socket.getpeername()
            except: pass

            if self.sslConnection and type(self.socket)!=ssl.SSLSocket:
                self.wrapSocket()
                
    def initSocket(self,host,port):
        self.host = host
        self.port = port
        try:
            self.socket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            self.socket.connect( (self.host,self.port) )
            self.socket.settimeout(self.timeout)

            if self.sslConnection:
                self.wrapSocket()
        except:
            self.connectionError = True
            if self.catchErrors:
                sys.stderr.write("hSocket: Could not establish an ssl connection to %s:%s\n" % (self.host,self.port))
                sys.stderr.write("  ERROR: %s\n" % sys.exc_info()[1])
                #sys.stderr.write("-------------------------\n")
            else:
                raise

    def wrapSocket(self):
        # wrap socket as ssl socket
        try:
            self.socket = ssl.wrap_socket(self.socket,
                                          server_side = self.serverSideSSLConn,
                                          certfile = self.certfile,
                                          keyfile = self.keyfile,
                                          ca_certs = self.ca_certs,
                                          cert_reqs = ssl.CERT_REQUIRED,
                                          ssl_version = ssl.PROTOCOL_SSLv3)
            
        except ssl.SSLError,msg:
            traceback.print_exc(file=sys.stdout)
            self.connectionError = True
            if self.catchErrors:
                sys.stderr.write("[%s] hSocket: Could not connect to %s:%s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),self.host,self.port))
                sys.stderr.write("  ERROR: %s\n" % sys.exc_info()[1])
                #sys.stderr.wrtie("TRACBACK:")
                #traceback.print_exc(file=sys.stderr)
                #sys.stderr.write("-------------------------\n")
                

        
    def recv(self):
        # read string from socket
        try:
            h,p = self.socket.getpeername()
            self.receivedStr = ""

            if self.EOCString:
                m = re.compile( "{eoc}$".format(eoc=self.EOCString) )
                
                # listen to socket until EOC string appears
                while 1:
                    if self.logFileIn:
                        self.logFileIn.write("[%s] [%s:%s] reading from socket ...\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
                        self.logFileIn.flush()

                    s = self.socket.recv(1024)
                    if not s:
                        # probably socket has been closed
                        break
                    elif m.search(self.receivedStr+s):
                        #s = s.replace(self.EOCString,"")
                        self.receivedStr += s
                        self.receivedStr = m.sub("",self.receivedStr)
                        break

                    self.receivedStr += s
            else:
                self.receivedStr = self.socket.recv(2048)

            if self.logFileIn:
                if self.receivedStr:
                    #self.logFileIn.write("["+datetime.now().strftime("%Y.%m.%d %H:%M:%S")+"] ["+h+":"+str(p)+"] In ["+h+":"+p+"]:  "+self.receivedStr+"\n")
                    self.logFileIn.write("[%s] [%s:%s] In: %s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p,self.receivedStr))
                    self.logFileIn.flush()

            return self.receivedStr
        except socket.timeout:
            self.connectionError = True
            if self.catchErrors:
                h,p = ("","")
                # get peername if possible
                try:
                    h,p = self.socket.getpeername()
                except:
                    pass

                sys.stderr.write("[%s] hSocket: Timeout on socket %s:%s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
            else:
               raise 
        except ssl.SSLError:
            self.connectionError = True
            if self.catchErrors:
                h,p = ("","")
                # get peername if possible
                try:
                    h,p = self.socket.getpeername()
                except:
                    pass

                sys.stderr.write("[%s] [%s:%s] hSocket: SSL error after request: %s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
                                                                                    h,
                                                                                    p,
                                                                                    self.sentStr))
            else:
               raise 
        except:
            self.connectionError = True
            if self.catchErrors:
                # traceback object
                tb = sys.exc_info()
                
                h,p = ("?","?")
                # get peername if possible
                try:
                    h,p = self.socket.getpeername()
                except:
                    pass

                sys.stderr.write("[%s] hSocket: Error while receiving something from %s:%s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
                #sys.stderr.write("  ERROR: %s\n" % sys.exc_info()[1])
                traceback.print_exception(*tb,file=sys.stderr)
                #sys.stderr.write("TRACBACK:\n")
                #traceback.print_exc(file=sys.stderr)
                #sys.stderr.write("-------------------------\n")
            else:
                raise
                
    def send(self,s):
        try:
            # send string to socket
            if self.EOCString:
                ss = s + self.EOCString

            self.socket.send(ss)
                
            if self.logFileOut:
                if s:
                    h,p = self.socket.getpeername()
                    outStr = s.split('\n')[0][:60]
                    if len(outStr)<len(s):
                        outStr += '[...][%s]' % len(s)
                    self.logFileOut.write("[%s] [%s:%s] Out: %s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p,outStr))
                    self.logFileOut.flush()
                    
            self.sentStr = s

        except socket.timeout:
            self.connectionError = True
            if self.catchErrors:
                h,p = ("","")
                # get peername if possible
                try:
                    h,p = self.socket.getpeername()
                except:
                    pass

                sys.stderr.write("hSocket: Timeout on socket %s:%s\n" % peername)
        except:
            self.connectionError = True
            if self.catchErrors:
                # traceback object
                tb = sys.exc_info()
            
                h,p = ("?","?")
                # get peername if possible
                try:
                    h,p = self.socket.getpeername()
                except:
                    pass

                sys.stderr.write("[%s] hSocket: Error while sending something from %s:%s\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
                #sys.stderr.write("  ERROR: %s\n" % sys.exc_info()[1])
                traceback.print_exception(*tb,file=sys.stderr)
                #sys.stderr.write("TRACBACK:")
                #traceback.print_exc(file=sys.stderr)
                #sys.stderr.write("-------------------------\n")
            else:
                sys.stderr.write("-----------error not catched\n")
                raise

    def close(self):
        # close socket
        h,p = self.socket.getpeername()
        self.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        if self.logFileOut:
            #self.logFileOut.write("[%s] [%s:%s] close socket\n" % (datetime.now().strftime("%Y.%m.%d %H:%M:%S"),h,p))
            pass

    def shutdown(self,SIG):
        self.socket.shutdown(SIG)
