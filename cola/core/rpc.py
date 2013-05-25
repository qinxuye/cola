#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

class ColaRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        self.allow_none = True