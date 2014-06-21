#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2013 Qin Xuye <qin@qinxuye.me>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Created on 2013-5-23

@author: Chine
'''

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import os
import socket
import threading

RETRY_TIMES = 10

class ColaRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True
        self.allow_reuse_address = True
        
    def register_function(self, function, name=None, prefix=None):
        if prefix is not None:
            if name is None:
                name = function.__name__
            prefix = prefix+'_' if not prefix.endswith('_') else prefix
            SimpleXMLRPCServer.register_function(self, function, name=prefix+name)
        else:
            SimpleXMLRPCServer.register_function(self, function, name=name)
        
class ThreadedColaRPCServer(object):
    
    def __init__(self, *args, **kwargs):
        self.rpc_server = ColaRPCServer(*args, **kwargs)
        self._t = threading.Thread(target=self.rpc_server.serve_forever)
        self._t.setDaemon(True)
        self._t.start()
        
    def register_function(self, function, name=None, prefix=None):
        self.rpc_server.register_function(function, name=name,
                                          prefix=prefix)
        
    def shutdown(self):
        self.rpc_server.shutdown()
        self._t.join()
        
def client_call(server, func_name, *args, **kwargs):
    serv = xmlrpclib.ServerProxy('http://%s' % server)
    ignore = kwargs.get('ignore', False)
    if not ignore:
        err = None
        retry_times = 0
        while retry_times <= RETRY_TIMES:
            try:
                return getattr(serv, func_name)(*args)
            except socket.error, e:
                retry_times += 1
                err = e
        raise err
    else:
        try:
            return getattr(serv, func_name)(*args)
        except socket.error:
            pass

class FileTransportServer(object):
    def __init__(self, rpc_server, dirname):
        self.rpc_server = rpc_server
        self.dirname = dirname
        self.rpc_server.register_function(self.receive_file)
        
    def receive_file(self, name, args):
        path = os.path.join(self.dirname, name)
        with open(path, 'wb') as handle:
            handle.write(args.data)
            return True
        
class FileTransportClient(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path
        
    def send_file(self):
        name = os.path.split(self.path)[1]
        with open(self.path, 'rb') as handle:
            binary_data = xmlrpclib.Binary(handle.read())
            client_call(self.server, 'receive_file', name, binary_data)