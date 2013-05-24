#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-24

@author: Chine
'''

import socket

def get_ip():
    localIP = socket.gethostbyname(socket.gethostname())
    ex = socket.gethostbyname_ex(socket.gethostname())[2]
    for ip in ex:
        if ip != localIP:
            return ip