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

Created on 2013-5-31

@author: Chine
'''

import logging.handlers
import SocketServer
import struct
try:
    import cPickle as pickle
except ImportError:
    import pickle

def get_logger(filename=None, server=None, name='cola'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    
    if filename is not None:
        handler = logging.FileHandler(filename)
        formatter = logging.Formatter('%(asctime)s - %(module)s.%(funcName)s.%(lineno)d - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    if server is not None:
        if ':' in server:
            server, port = tuple(server.split(':', 1))
        else:
            port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
            
        socket_handler = logging.handlers.SocketHandler(server, port)
        socket_handler.setLevel(logging.ERROR)
        logger.addHandler(socket_handler)
    
    return logger

class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        while not self.server.abort:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unPickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handleLogRecord(record)
            
    def unPickle(self, data):
        return pickle.loads(data)
    
    def handleLogRecord(self, record):
        if self.server.logger is not None:
            logger = self.server.logger
        else:
            logger = logging.getLogger(record.name)
        logger.handle(record)
        
class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
    
    allow_reuse_address = 1
    
    def __init__(self, logger=None, host='localhost', 
                 port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
                 handler=LogRecordStreamHandler):
        SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = False
        self.timeout = 1
        self.logger = logger
        
    def stop(self):
        self.abort = True