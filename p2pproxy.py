#encoding=utf-8
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
import logging

logging.basicConfig(
    filename='p2pproxy.log',
    format='%(asctime)s <%(name)s> [%(levelname)s]:%(message)s',
    level=logging.INFO)
    
P2P_CMD_LOGIN = 1
P2P_CMD_DATA = 2
P2P_CMD_LOGOUT = 3
P2P_CMD_CLIENT = 4
P2P_CMD_TIMER = 5
    
def parse_address(address):
    try:
        hostname, port = address.rsplit(':', 1)
        port = int(port)
    except ValueError:
        sys.exit('Expected HOST:PORT: %r' % address)
    return gethostbyname(hostname), port
    
    
class P2pSession:
    def __init__(self, sock):
        self.sock = sock
        self.queue = Queue()
        self.loop = True 
        self.last_read_time = time.time()
        
    def is_timeout(self, seconds):
        return time.time() - self.last_read_time > seconds
            
    def is_loop(self):
        return self.loop
    
    def break_loop(self):
        self.loop = False    
        
    def read(self, count):
        data = self.sock.recv(count)  
        if data is None or len(data) == 0:
            return data
        
        self.last_read_time = time.time()
        
        return data
        

    def write(self, data):
        self.queue.put(data)
            
    def write_loop(self):
        ret = False
        
        try:
            while True:
                data = self.queue.get()
                if not self.is_loop():
                    break
                    
                self.sock.sendall(data)
                
            ret = True
        except:        
            self.break_loop()
        
        return ret
        
    def close(self):
        self.loop = False        
        if self.sock != None:            
            self.sock.close()            
            self.queue.put(1) # break queue.get()
            
            self.sock = None
        
class P2pClient:
    def __init__(self, src, dst):
        """
            src : p2p server host
            dst : real server host
        """
        self.src = src
        self.dst = dst
        
        self.clients = {}
        self.session = None
        
    def start(self):
        gevent.spawn(self.connect)  
    
    def connect(self):
        try:
            sock = create_connection(self.src)
        except IOError as ex:
            logging.error('P2pClient failed to connect to %s' % str(self.src))
            return  
        
        logging.info('P2pClient connect to server %s' % str(self.src))
        
        self.clients = {}
        self.session = P2pSession(sock)
        
        try:
            r = gevent.spawn(self.onread)
            w = gevent.spawn(self.onwrite)
            t = gevent.spawn(self.ontimer)

            gevent.joinall([r, w, t])
            
        finally:
            self.session = None 
            
        logging.info('P2pClient disconnect to server %s' %  str(self.src))
    
    def onread(self):
        try:   
            self.session.write(struct.pack('iii', 0, 0, P2P_CMD_CLIENT))
            gevent.sleep(1)            
                  
            count = 0
            clientid = 0
            cmd = 0
            
            while self.session.is_loop():
                if count > 0:
                    data = self.session.read(count)
                    if not data:
                        break   
             
                    count -= len(data)
                    if cmd == P2P_CMD_DATA:                     
                        self.request_data(clientid, data)
                else:       
                    ### parse header
                    data = self.session.read(12)
                    if not data:
                        break                        
                  
                    count, clientid, cmd = struct.unpack('iii', data)
                    logging.debug('count[%d], clientid[%d], cmd[%d]' % (count, clientid, cmd))
                    
                    if cmd == P2P_CMD_LOGIN:                    
                        if count > 0:
                            logging.error ("P2pClient onread login count[%d] is error" % count)
                            break
                            
                        self.append_client(clientid)
                    elif cmd == P2P_CMD_LOGOUT:
                        if count > 0:
                            logging.error ("P2pClient onread logout count[%d] is error" % count)
                            break
                            
                        self.remove_client(clientid)
                    elif cmd == P2P_CMD_TIMER:
                        if count > 0:
                            logging.error ("P2pClient onread timer count[%d] is error" % count)
                            break
                    else:
                        if count >= 1024*1024:
                            logging.error ("P2pClient onread count[%d] is too big" % count)
                            break
                        else:
                            continue
        except IOError as ex:
            logging.error('P2pClient onread exception' + str(ex))
        except:
            logging.error ('P2pClient onread exception')
        finally:
            self.session.close()
    
    def onwrite(self):
        self.session.write_loop()
        
    def ontimer(self):
        try:  
            count = 10            
            while self.session.is_loop(): 
                gevent.sleep(1)  
                
                if count > 0:
                    count -= 1
                    
                if count == 0 and self.session.is_timeout(30):
                    self.session.write(struct.pack('iii', 0, 0, P2P_CMD_TIMER))
                    count = 10                 
        except:
            logging.error ('P2pClient ontimer error')        
     
    def response_data(self, clientid, data):
        if data is None:
            return
            
        self.session.write(struct.pack('iii', len(data), clientid, P2P_CMD_DATA)) 
        self.session.write(data)
        
    def request_data(self, clientid, data):    
        if self.clients.has_key(clientid):
            self.clients[clientid].write(data)
        else:
            logging.warning ('P2pClient request_data client[%d] is missing' % clientid)
            
    def append_client(self, clientid):
        logging.info('P2pClient client[%d] login' % clientid)
        
        gevent.spawn(self.connect_client, clientid)
    
    def remove_client(self, clientid):
        logging.info ('P2pClient client[%d] logout' % clientid)
        
        try:
            if self.clients.has_key(clientid):
                ss = self.clients[clientid]                
                del self.clients[clientid]
                
                ss.close()
        except:
            logging.error('P2pClient remove client[%d] falied' % clientid)
        
    def connect_client(self, clientid):
        try:
            sock = create_connection(self.dst)
        except IOError as ex:
            logging.error('P2pClient failed to connect to %s' % str(self.dst))
            return
        
        ss = P2pSession(sock)
        self.clients[clientid] = ss
        gevent.sleep(1)
        
        try:
            r = gevent.spawn(self.onclientread, ss, clientid)
            w = gevent.spawn(self.onclientwrite, ss)

            gevent.joinall([r,w])

        finally:
            self.remove_client(clientid)
            
    def onclientread(self, ss, clientid):
        try:
            while ss.is_loop():
                data = ss.read(4096)
                if not data:
                    break
                    
                self.response_data(clientid, data)   
                
            self.session.write(struct.pack('iii', 0, clientid, P2P_CMD_LOGOUT))   
            
        except:
            ss.break_loop()
            logging.error ('P2pClient client[%d] read failed' % clientid)  
    
    def onclientwrite(self, ss):
        ss.write_loop()
                        
    def close(self):
        for i in self.clients.keys:
            self.remove_client(i)
        
        if self.session != None:
            self.session.close()
            self.session = None
         
class P2pServer(StreamServer):
    def __init__(self, listener, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        
        self.netserver = None
        self.client = None
    
    def sendcmd(self, clientid, cmd, length = 0):
        if self.client is None:
            return
            
        self.client.write(struct.pack('iii', length, clientid, cmd))    
            
    def senddata(self, clientid, data):
        if self.client is None:
            return
            
        self.sendcmd(clientid, P2P_CMD_DATA, len(data))    
        self.client.write(data)

    def handle(self, sock, address): 
        addr = '%s:%d' % ( address[0], address[1])
        
        logging.info("P2pServer client %s connect" % addr)
        
        if (self.client != None):
            sock.close()
            logging.warning ('P2pServer client is not none.')
            return

        session = P2pSession(sock)         
        try:
            r = gevent.spawn(self.onread, session)
            w = gevent.spawn(self.onwrite, session)
            t = gevent.spawn(self.ontimer, session)

            gevent.joinall([r,w,t])

        finally:
            if self.client == session:
                self.client = None     
            
        logging.info ('P2pServer client %s disconnect.' % addr)

    def onread(self, session):
        try:
            if self.verify_client(session):
            
                if self.client != None:
                    logging.warning ('P2pServer client is not none.')
                    return
                    
                #  verify is ok, set client 
                self.client = session
                
                count, clientid, cmd = 0, 0, 0
                while session.is_loop():
                    if count > 0:
                        data = session.read(count)
                        if not data:
                            break
                            
                        count -= len(data)
                        if cmd == P2P_CMD_DATA:
                            self.netserver.senddata(clientid, data)
                    else:
                        ### parse header
                        data = session.read(12)
                        if not data:
                            break
                            
                        count, clientid, cmd = struct.unpack('iii', data)   
                        self.last_recv_time = time.time()   
                        
                        if cmd == P2P_CMD_LOGOUT:                            
                            if count > 0:                                   
                                logging.error ("P2pServer logout count[%d] is error" % count)
                                break
                                
                            self.netserver.shutdown_client(clientid)
                            
                        elif cmd == P2P_CMD_TIMER:                            
                            self.sendcmd(0, P2P_CMD_TIMER)
                            if count > 0:                                   
                                logging.error ("P2pServer timer count[%d] is error" % count)
                                break
        except:
            logging.error ("P2pServer onread sock is exception")
            
        finally:
            session.close()

    def onwrite(self, session):
        if session is None:
            return

        session.write_loop()
    
    def ontimer(self, session):
        if session is None:
            return
    
        try:
            while session.is_loop():          
                gevent.sleep(1)   
                if session.is_timeout(120):                  
                    session.break_loop()
                    logging.warning ('P2pServer client timeout')    
        except:
            logging.error ('P2pServer ontimer exception')           

    def verify_client(self, session):
        try:
            data = session.read(12)
            if data != None:
                count, clientid, cmd = struct.unpack('iii', data)
                if count == 0 and cmd == P2P_CMD_CLIENT and clientid == 0:
                    logging.info ('P2pServer client login')
                    return True
        except:
            pass
            
        logging.warning("P2pServer verify_client failed")
        
        return False        
    
            
    def close(self):
        if self.client != None:
            self.client.break_loop()
            
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
        
        logging.info ('NetServer %s connect.' % addr)
        
        session = P2pSession(sock)
        
        self.id += 1
        clientid = self.id
        self.clients[clientid] = session
        
        try:
            r = gevent.spawn(self.onread, session, clientid)
            w = gevent.spawn(self.onwrite, session)

            gevent.joinall([r,w])

        finally:
            self.remove_client(clientid)            
        
        logging.info ('NetServer %s disconnect.' % addr)

    def onread(self, session, clientid):
        try:
            self.p2pserver.sendcmd(clientid, P2P_CMD_LOGIN)   
            gevent.sleep(1)
            while session.is_loop():
                try:
                    data = session.read(4096)
                    if not data:
                        break
                    
                    self.p2pserver.senddata(clientid, data)
                except socket.error:
                    break                    
            self.p2pserver.sendcmd(clientid, P2P_CMD_LOGOUT)  
        except:
            pass 
        finally:
            session.close()    

    def onwrite(self, session):
        session.write_loop()
        
    def close(self):
        for i in self.clients.keys:
            self.clients[clientid].break_loop()     
            
        StreamServer.close(self)

    def senddata(self, clientid, data):
        if self.clients.has_key(clientid):
            self.clients[clientid].write(data)
    
    def shutdown_client(self, clientid):
        if self.clients.has_key(clientid):
            self.clients[clientid].close()
            
    def remove_client(self, clientid):
        logging.info ('NetServer client[%d] logout' % clientid)
        
        try:
            if self.clients.has_key(clientid):
                ss = self.clients[clientid]                
                del self.clients[clientid]
                
                ss.close()
        except:
            logging.error ('NetServer remove client[%d] exception' % clientid)        
         
def client_loop(p2phost, serverhost):

    p2phost = parse_address(p2phost)
    serverhost = parse_address(serverhost) 

    while True:
        client = P2pClient(p2phost, serverhost)   
        
        gevent.signal(signal.SIGTERM, client.close)
        gevent.signal(signal.SIGINT, client.close)
        
        client.start()  
        gevent.wait()      
        
        time.sleep(8)        
        
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
    
