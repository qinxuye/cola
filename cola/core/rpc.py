#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

class ColaRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True
    
        
def client_call(server, func_name, *args):
    serv = xmlrpclib.ServerProxy('http://%s' % server)
    return getattr(serv, func_name)(*args)

class FileTransportServer(object):
    def __init__(self, rpc_server, path):
        self.rpc_server = rpc_server
        self.path = path
        self.rpc_server.register_function(self.receive_file)
        
    def receive_file(self, args):
        with open(self.path, 'wb') as handle:
            handle.write(args.data)
            return True
        
class FileTransportClient(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path
        
    def send_file(self):        
        with open(self.path, 'rb') as handle:
            binary_data = xmlrpclib.Binary(handle.read())
            client_call(self.server, 'receive_file', binary_data)