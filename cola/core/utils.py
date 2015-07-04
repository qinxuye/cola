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

Created on 2013-5-24

@author: Chine
'''

import socket
import os
import sys
import urllib
import multiprocessing
import time
import platform
import tempfile
import shutil
from multiprocessing.reduction import reduce_connection
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.errors import DependencyNotInstalledError

def add_localhost(func):
    def inner(*args, **kwargs):
        ips = func(*args, **kwargs)
        localhost = '127.0.0.1'
        if localhost not in ips:
            ips.append(localhost)
        return ips
    return inner

@add_localhost
def get_ips():
    localIP = socket.gethostbyname(socket.gethostname())
    ex = socket.gethostbyname_ex(socket.gethostname())[2]
    if len(ex) == 1:
        return [ex[0]]
    return [ip for ip in ex if ip != localIP]

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

def import_job_desc(path):
    dir_, name = os.path.split(path)
    if os.path.isfile(path):
        name = name.rstrip('.py')
    else:
        sys.path.insert(0, os.path.dirname(dir_))
    sys.path.insert(0, dir_)
    job_module = __import__(name)
    job_desc = job_module.get_job_desc()
    
    return job_desc

def urldecode(link):
    decodes = {}
    if '?' in link:
        params = link.split('?')[1]
        for param in params.split('&'):
            k, v = tuple(param.split('='))
            decodes[k] = urllib.unquote(v)
    return decodes

def beautiful_soup(html, logger=None):
    try:
        from bs4 import BeautifulSoup, FeatureNotFound
    except ImportError:
        raise DependencyNotInstalledError("BeautifulSoup4")
    
    try:
        return BeautifulSoup(html, 'lxml')
    except FeatureNotFound:
        if logger is not None:
            logger.warning('lxml not installed')
        return BeautifulSoup(html)
    
def iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    
    return True

def get_rpc_prefix(app_name=None, prefix=None):
    if app_name and not app_name.endswith('_'):
        app_name += '_'
    elif not app_name:
        app_name = ''
        
    if prefix and not prefix.endswith('_'):
        prefix += '_'
    elif not prefix:
        prefix = ''
        
    return app_name + prefix

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def base58_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)

def get_cpu_count():
    return multiprocessing.cpu_count()

def get_os_name():
    return platform.system()

def is_windows():
    return get_os_name() == 'Windows'

class Clock(object):
    def __init__(self, start=None):
        self.start = start
        if self.start is None:
            self.start = time.time()
            
        self.paused = 0.0
        self.is_paused = False
        self.acc_paused = 0.0
            
    def pause(self):
        if not self.is_paused:
            self.paused = time.time()
            self.is_paused = True
        
    def resume(self):
        if self.is_paused:
            self.acc_paused += time.time() - self.paused
            self.is_paused = False
        
    def clock(self):
        return time.time() - self.start - self.acc_paused

def pickle_connection(connection):
    return pickle.dumps(reduce_connection(connection))

def unpickle_connection(pickled_connection):
    (func, args) = pickle.loads(pickled_connection)
    return func(*args)

def import_module(module_name):
    root, module = module_name.rsplit('.', 1)
    return getattr(__import__(root, fromlist=['']), module)

def pack_local_job_error(job_name, working_dir=None, logger=None):
    if working_dir is None:
        working_dir = os.path.join(tempfile.gettempdir(), 'cola', 'worker', job_name)
    if not os.path.exists(working_dir):
        if logger is not None:
            logger.warning('job data does not exist, no error to pack')
        return
    
    pack_dir = os.path.join(working_dir, 'errors', 'gather')
    if os.path.exists(pack_dir):
        shutil.rmtree(pack_dir)
    if not os.path.exists(pack_dir):
        os.makedirs(pack_dir)

    for name in os.listdir(working_dir):
        path = os.path.join(working_dir, name)

        # the instance file
        if os.path.isdir(path) and name.isdigit():
            error_dir = os.path.join(path, 'errors')
            if os.path.exists(error_dir):
                for error_detail_dir in os.listdir(error_dir):
                    shutil.copytree(os.path.join(error_dir, error_detail_dir),
                                    os.path.join(pack_dir, error_detail_dir))
                
    return pack_dir