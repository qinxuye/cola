#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

class ColaRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    pass