#encode=utf-8
#python2

import socket
import sys
import signal
import time
import struct
import array
import gevent
from gevent.queue import Queue
from gevent.server import StreamServer
from gevent.socket import create_connection, gethostbyname

P2P_CMD_LOGIN = 1
P2P_CMD_DATA = 2
P2P_CMD_LOGOUT = 3
P2P_CMD_CLIENT = 4
P2P_CMD_TIMER = 5

def log(message, *args):
    message = message % args
    sys.stderr.write(message + '\n')
    
def parse_address(address):
    try:
        hostname, port = address.rsplit(':', 1)
        port = int(port)
    except ValueError:
        sys.exit('Expected HOST:PORT: %r' % address)
    return gethostbyname(hostname), port
    
class P2pClient:
    def __init__(self, src, dst):
        """
            src : p2p server host
            dst : real server host
        """
        self.src = src
        self.dst = dst
        self.sock = None  
        self.clients = {}
        self.queue = None
        self.loop_run = True
        
    def start(self):
        self.loop_run = True
        gevent.spawn(self.connect)  
    
    def connect(self):
        try:
            self.sock = create_connection(self.src)
        except IOError as ex:
            log('[erro]: P2pClient failed to connect to %s', self.src)
            return   
        
        self.clients = {}
        self.queue = Queue()
        try:
            r = gevent.spawn(self.onread)
            w = gevent.spawn(self.onwrite)
            t = gevent.spawn(self.ontimer)

            gevent.joinall([r, w, t])

        finally:
            self.sock = None 
            self.queue = None
    
    def onread(self):
        try:   
            self.queue.put(struct.pack('iii', 0, 0, P2P_CMD_CLIENT))
            gevent.sleep(1)
                  
            count = 0
            clientid = 0
            cmd = 0
            
            while self.loop_run:
                if count > 0:
                    data = self.sock.recv(count)
                    if not data:
                        break   
                    
             
                    count -= len(data)
                    if cmd == P2P_CMD_DATA:                     
                        self.requestData(clientid, data)
                else:       
                    ### parse header
                    data = self.sock.recv(12)
                    if not data:
                        break
                        
                  
                    count, clientid, cmd = struct.unpack('iii', data)
                    #print 'count[%d], clientid[%d], cmd[%d]' % (count, clientid, cmd)
                    
                    if cmd == P2P_CMD_LOGIN:                    
                        if count > 0:
                            log ("[erro]: P2pClient onread login count[%d] is error", count)
                            break
                            
                        self.appendClient(clientid)
                    elif cmd == P2P_CMD_LOGOUT:
                        if count > 0:
                            log ("[erro]: P2pClient onread logout count[%d] is error", count)
                            break
                            
                        self.removeClient(clientid)
                    elif cmd == P2P_CMD_TIMER:
                        if count > 0:
                            log ("[erro]: P2pClient onread timer count[%d] is error", count)
                            break
                    else:
                        if count >= 1024*1024:
                            log ("[erro]: P2pClient onread count[%d] is too big", count)
                            break
                        else:
                            continue
        except IOError as ex:
            log(str(ex))
        except:
            log ('[erro]: P2pClient onread exception')
        finally:
            self.loop_run = False
            
            if self.sock != None:
                self.sock.close()
                self.queue.put('1')    

        log ('[info]: P2pClient onread finished')
    
    def onwrite(self):
        try:
            while self.loop_run:
                data = self.queue.get()
                self.sock.sendall(data)
        except:
            log ('[erro]: P2pClient onwrite error')
            self.loop_run = False

        log ('[info]: P2pClient write finished')
     
    def responseData(self, clientid, data):
        if data is None:
            return
            
        self.queue.put(struct.pack('iii', len(data), clientid, P2P_CMD_DATA)) 
        self.queue.put(data)
        
    def requestData(self, clientid, data):
    
        if self.clients.has_key(clientid):
            self.clients[clientid][1].put(data)
        else:
            log ('[warn]: P2pClient requestData client[%d] is missing', clientid)
            
    def appendClient(self, clientid):
        log ('[info]: P2pClient client[%d] login', clientid)
        
        gevent.spawn(self.connectClient, clientid)
    
    def removeClient(self, clientid):
        log ('[info]: P2pClient client[%d] logout', clientid)
        
        try:
            if self.clients.has_key(clientid):
                sock = self.clients[clientid][0]  
                q = self.clients[clientid][1]  
                del self.clients[clientid]      
                sock.close()
                q.put('1')
        except:
            log ('[erro]: P2pClient remove client[%d] falied', clientid)
        
    def connectClient(self, clientid):
        try:
            sock = create_connection(self.dst)
        except IOError as ex:
            log('[erro]: P2pClient failed to connect to %s', self.dst)
            return
        
        q = Queue()
        self.clients[clientid] = (sock, q)
        gevent.sleep(1)
        
        try:
            r = gevent.spawn(self.onclientread, sock, clientid)
            w = gevent.spawn(self.onclientwrite, sock, q)

            gevent.joinall([r,w])

        finally:
            self.removeClient(clientid)
            
    def onclientread(self, sock, clientid):
        try:
            while True:
                data = sock.recv(1024)
                if not data:
                    break
                    
                self.responseData(clientid, data)   
                
            self.queue.put(struct.pack('iii', 0, clientid, P2P_CMD_LOGOUT))   
            
        except:
            log ('[erro]: P2pClient client[%d] read failed', clientid)    
    
    def onclientwrite(self, sock, q):
        try:
            while True:
                data = q.get()
                sock.sendall(data)
        except:
            log ('[erro]: P2pClient onclientwrite failed')      
                        
    def close(self):
        for i in self.clients.keys:
            self.removeClient(i)
        
        if self.sock != None:
            self.sock.close()
            self.queue.put('1')

    def ontimer(self):
        try:
            while self.loop_run:
                gevent.sleep(30)
                self.queue.put(struct.pack('iii', 0, 0, P2P_CMD_TIMER))                
        except:
            log ('[erro]: P2pClient ontimer error')
            
class P2pServer(StreamServer):
    def __init__(self, listener, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        
        self.netserver = None
        self.client = None
        self.clientqueue = None

    def sendcmd(self, clientid, cmd, length = 0):
        senddata = struct.pack('iii', length, clientid, cmd)
        self.clientqueue.put(senddata)    
            
    def senddata(self, clientid, data):
        self.sendcmd(clientid, 2, len(data))    
        self.clientqueue.put(data)

    def handle(self, source, address): 
        addr = '%s:%d'%(address[0],address[1])
        log("[info]: P2pServer %s connect", addr)
        
        if (self.client != None):
            source.close()
            return

        self.clientqueue = Queue()
        self.client = source
        
        try:
            r = gevent.spawn(self.onread)
            w = gevent.spawn(self.onwrite)

            gevent.joinall([r,w])

        finally:
            self.clientqueue = None
            self.client = None        
            log ('[info]: P2pServer %s disconnect.', addr)

    def onread(self):
        try:
            if self.verifyclient():
                count, clientid, cmd = 0, 0, 0
                while True:
                    if count > 0:
                        data = self.client.recv(count)
                        if not data:
                            break
                        
                        count -= len(data)
                        if cmd == P2P_CMD_DATA:
                            self.netserver.senddata(clientid, data)
                    else:
                        ### parse header
                        data = self.client.recv(12)
                        if not data:
                            break
                            
                        count, clientid, cmd = struct.unpack('iii', data)                        
                        if cmd == P2P_CMD_LOGOUT:                            
                            if count > 0:                                   
                                log("[erro]: P2pServer logout count[%d] is error", count)
                                break
                                
                            self.netserver.shutdownClient(clientid)
                            
                        elif cmd == P2P_CMD_TIMER:
                            self.clientqueue.put(struct.pack('iii', 0, 0, P2P_CMD_TIMER))
                            if count > 0:                                   
                                log("[erro]: P2pServer timer count[%d] is error", count)
                                break
        except:
            log ("[erro]: P2pServer onread sock is disconnected")
            
        finally:
            self.client.close()
            self.clientqueue.put('1')

    def onwrite(self):
        try:
            while True:
                data = self.clientqueue.get()        
                self.client.sendall(data)
        except:
            log ("[erro]: P2pServer onwrite sock is disconnected")

    def verifyclient(self):
        try:
            data = self.client.recv(12)
            if data != None:
                count, clientid, cmd = struct.unpack('iii', data)
                if count == 0 and cmd == P2P_CMD_CLIENT and clientid == 0:
                    log ('[info]: P2pServer client login')
                    return True
        except:
            pass
            
        log("[warn]: P2pServer verifyclient failed")
        
        return False        
    
            
    def close(self):
        StreamServer.close(self)            
  
class NetServer(StreamServer):
    def __init__(self, listener, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        
        self.p2pserver = None  
        self.id = 0
        self.clients = {}

    def handle(self, sock, address):
        addr = '%s:%d' %( address[0], address[1])
        
        if (self.p2pserver.client == None):
            sock.close()
            return

        q = Queue()
        
        self.id += 1
        clientid = self.id
        self.clients[clientid] = (sock, q)
        
        try:
            r = gevent.spawn(self.onread, sock, q, clientid)
            w = gevent.spawn(self.onwrite, sock, q)

            gevent.joinall([r,w])

        finally:
            del self.clients[clientid]            
            log ('[info]: %s disconnect.', addr)

    def onread(self, sock, queue, clientid):
        try:
            self.p2pserver.sendcmd(clientid, P2P_CMD_LOGIN)   
            gevent.sleep(1)
            while True:
                try:
                    data = sock.recv(4096)
                    if not data:
                        break
                    
                    self.p2pserver.senddata(clientid, data)
                except socket.error:
                    break                    
            self.p2pserver.sendcmd(clientid, P2P_CMD_LOGOUT)  
        except:
            pass 
        finally:
            sock.close()
            queue.put('1')        

    def onwrite(self, sock, queue):
        try:
            while True:
                data = queue.get()
                sock.sendall(data)
        except:      
            pass
        
    def close(self):
        StreamServer.close(self)

    def senddata(self, clientid, data):
        if self.clients.has_key(clientid):
            self.clients[clientid][1].put(data)
    
    def shutdownClient(self, clientid):
        if self.clients.has_key(clientid):
            self.clients[clientid][0].close()

          
def client_loop(p2phost, serverhost):

    p2phost = parse_address(p2phost)
    serverhost = parse_address(serverhost) 

    while True:
        client = P2pClient(p2phost, serverhost)   
        
        client.start()  
        gevent.wait()      
        
        time.sleep(5)        
        
def server_loop(p2phost, proxyhost):

    p2phost = parse_address(p2phost)       
    proxyhost = parse_address(proxyhost)
    
    netserver = NetServer(proxyhost)
    p2pserver = P2pServer(p2phost)
    
    netserver.p2pserver = p2pserver
    p2pserver.netserver = netserver
    
    gevent.signal(signal.SIGTERM, netserver.close)
    gevent.signal(signal.SIGINT, netserver.close)
    gevent.signal(signal.SIGTERM, p2pserver.close)
    gevent.signal(signal.SIGINT, p2pserver.close)
    
    p2pserver.start()
    netserver.start()
     
    gevent.wait()
    
 
def main():  
    s = """
            Usage:
            client mode: p2pproxy -c -p2p=host1 -server=host2
            server mode: p2pproxy -s -p2p=host1 -server=host2
            
            host: ip:port
        """
    args = sys.argv[1:] 
    if len(args) < 2:        
        sys.exit(s)
    
    parms = {}
    
    server_mode = True
    
    for arg in args:
        if len(arg) > 1 and arg[0] == '-':
            arr = arg[1:].split('=')
            if len(arr) == 1 and len(arg) == 2:
                if arg[1] == 'C' or arg[1] == 'c':
                    server_mode = False
                elif arg[1] == 'S' or arg[1] == 's':
                    server_mode = True
            elif len(arr) == 2:
                parms[arr[0]] = arr[1]                

    if len(parms) >= 2 and parms.has_key('p2p') and  parms.has_key('server'):
        p2p = parms['p2p']
        server = parms['server']
        
        if server_mode:
            print('start p2p server')
            server_loop(p2p, server)
        else:  
            print('start p2p client')
            client_loop(p2p, server)
    else:
        sys.exit(s)
    
if __name__ == '__main__':
    main()
    