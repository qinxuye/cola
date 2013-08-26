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

import socket
import logging.handlers
import SocketServer
import struct
try:
    import cPickle as pickle
except ImportError:
    import pickle
    
class Log(object):
    def __init__(self, name, default_level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(default_level)
        self.formatter = logging.Formatter(
            '%(asctime)s - %(module)s.%(funcName)s.%(lineno)d - %(levelname)s - %(message)s')
        
    def add_stream_log(self, level=logging.DEBUG):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        self.logger.addHandler(stream_handler)
        
    def add_file_log(self, filename, level=logging.INFO):
        handler = logging.FileHandler(filename)
        handler.setFormatter(self.formatter)
        handler.setLevel(level)
        self.logger.addHandler(handler)
        
    def add_remote_log(self, server, level=logging.INFO):
        if ':' in server:
            server, port = tuple(server.split(':', 1))
            port = int(port)
        else:
            port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
            
        socket_handler = logging.handlers.SocketHandler(server, port)
        socket_handler.setLevel(level)
        self.logger.addHandler(socket_handler)
        
    def get_logger(self):
        return self.logger

def get_logger(name='cola', filename=None, server=None, is_master=False, 
               basic_level=logging.INFO):
    log = Log(name, basic_level)
    log.add_stream_log(basic_level)
    
    if filename is not None:
        level = basic_level
        if is_master:
            level = logging.ERROR
        log.add_file_log(filename, level)
    
    if server is not None:
        log.add_remote_log(server, logging.INFO)
        
    return log.get_logger()

def add_log_client(logger, client):
    if ':' in client:
        client, port = tuple(client.split(':', 1))
        port = int(port)
    else:
        port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
        
    socket_handler = logging.handlers.SocketHandler(client, port)
    socket_handler.setLevel(logging.INFO)
    logger.addHandler(socket_handler)
    
    return socket_handler

class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        self.connection.setblocking(0)
        while not self.server.abort:
            try:
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
            except socket.error:
                return
            
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