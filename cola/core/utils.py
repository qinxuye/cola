#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-24

@author: Chine
'''

import socket
import os

def get_ip():
    localIP = socket.gethostbyname(socket.gethostname())
    ex = socket.gethostbyname_ex(socket.gethostname())[2]
    if len(ex) == 1:
        return ex[0]
    for ip in ex:
        if ip != localIP:
            return ip
        
def root_dir():
    def _get_dir(f):
        return os.path.dirname(f)
    f = os.path.abspath(__file__)
    for _ in range(3):
        f = _get_dir(f)
    return f