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

import os
import re
import threading
import mmap
import sys
from collections import defaultdict
import struct
import marshal
try:
    import cPickle as pickle
except ImportError:
    import pickle
    
from cola.core.utils import iterable
from cola.core.mq.utils import labelize

class StoreExistsError(Exception): pass

class StoreNotSafetyShutdown(Exception): pass

class StoreNoSpaceForPut(Exception): pass

STORE_FILE_SIZE = 4 * 1024 * 1024 # single store file must be less than 4M.
LEGAL_STORE_FILE_REGEX = re.compile("^\d+$")

READ_ENTRANCE, WRITE_ENTRANCE = range(2)

MARSHAL, PICKLE = 'm', 'p'

class Store(object):
    def __init__(self, working_dir, size=STORE_FILE_SIZE, 
                 deduper=None, mkdirs=False, 
                 create_lock_file=False):
        self.lock = threading.Lock()
        self.store_file_size = size
        self.deduper = deduper
        
        self.dir_ = working_dir
        if mkdirs and not os.path.exists(self.dir_):
            os.makedirs(self.dir_)
            
        self.create_lock_file = create_lock_file
        self.lock_file = os.path.join(self.dir_, 'lock')
        with self.lock:
            if create_lock_file is True:
                if os.path.exists(self.lock_file):
                    raise StoreExistsError('Directory is being used by another store.')
                else:
                    open(self.lock_file, 'w').close()

        self.stopped = False
        self.inited = False

        self.legal_files = []
        self.file_handles = defaultdict(lambda: None)
        self.map_handles = defaultdict(lambda: None)
            
    def shutdown(self):
        if self.stopped: return
        self.stopped = True
        
        try:
            for handle in self.map_handles.values():
                if handle is not None:
                    handle.close()
            for handle in self.file_handles.values():
                if handle is not None:
                    handle.close()
        finally:
            with self.lock:
                if self.create_lock_file is True:
                    os.remove(self.lock_file)
                
        self.inited = False
        
    def init(self):
        if self.inited: return
        
        files = os.listdir(self.dir_)
        for fi in files:
            if fi == 'lock': continue
            
            file_path = os.path.join(self.dir_, fi)
            if not os.path.isfile(file_path) or \
                LEGAL_STORE_FILE_REGEX.match(fi) is None:
                raise StoreNotSafetyShutdown('Store did not shutdown safety last time.')
            else:
                self.legal_files.append(file_path)
                
        self.legal_files = sorted(self.legal_files, key=lambda k: int(os.path.basename(k)))
        
        if len(self.legal_files) > 0:
            read_file_handle = self.file_handles[READ_ENTRANCE] \
                = open(self.legal_files[-1], 'r+')
            self.map_handles[READ_ENTRANCE] = mmap.mmap(read_file_handle.fileno(), 
                                                        self.store_file_size)
            if len(self.legal_files) == 1:
                self.file_handles[WRITE_ENTRANCE] = self.file_handles[READ_ENTRANCE]
                self.map_handles[WRITE_ENTRANCE] = self.map_handles[READ_ENTRANCE]
            else:
                write_file_handle = self.file_handles[WRITE_ENTRANCE] \
                    = open(self.legal_files[0], 'r+')
                self.map_handles[WRITE_ENTRANCE] = mmap.mmap(write_file_handle.fileno(),
                                                             self.store_file_size)
                
        self.inited = True
    
    def _generate_file(self):
        prev = None
        if len(self.legal_files) > 0:
            fn = os.path.basename(self.legal_files[0])
            prev = int(LEGAL_STORE_FILE_REGEX.match(fn).group())
        current = str(prev-1 if prev is not None else sys.maxint)
        file_path = os.path.join(self.dir_, current)
        if len(self.legal_files) > 1:
            self.map_handles[WRITE_ENTRANCE].close()
            self.file_handles[WRITE_ENTRANCE].close()
        self.legal_files.insert(0, file_path)
        open(file_path, 'w').close()
        write_file_handle = self.file_handles[WRITE_ENTRANCE] = open(file_path, 'r+')
        write_file_handle.write('\x00'*self.store_file_size)
        write_file_handle.flush()
        self.map_handles[WRITE_ENTRANCE] = mmap.mmap(write_file_handle.fileno(),
                                                     self.store_file_size)
        
        if len(self.legal_files) == 1:
            self.map_handles[READ_ENTRANCE] = self.map_handles[WRITE_ENTRANCE]
            self.file_handles[READ_ENTRANCE] = self.file_handles[WRITE_ENTRANCE]
        
    def _destroy_file(self):
        if len(self.legal_files) == 0:
            return
        self.map_handles[READ_ENTRANCE].close()
        self.file_handles[READ_ENTRANCE].close()
        if len(self.legal_files) == 1:
            self.map_handles[WRITE_ENTRANCE].close()
            self.file_handles[WRITE_ENTRANCE].close()
            self.map_handles.clear()
            self.file_handles.clear()
        elif len(self.legal_files) == 2:
            self.map_handles[READ_ENTRANCE] = self.map_handles[WRITE_ENTRANCE]
            self.file_handles[READ_ENTRANCE] = self.file_handles[WRITE_ENTRANCE]
        else:
            read_file_handle = self.file_handles[READ_ENTRANCE] \
                = open(self.legal_files[-2], 'r+')
            self.map_handles[READ_ENTRANCE] = mmap.mmap(read_file_handle.fileno(),
                                                        self.store_file_size)
        self.legal_files.pop(-1)
        
    def _stringfy(self, obj):
        try:
            return MARSHAL + marshal.dumps(obj)
        except ValueError:
            return PICKLE + pickle.dumps(obj)
        
    def _destringfy(self, src_str):
        if len(src_str) < 2:
            raise ValueError('String length must be at least 2.')
        
        t, str_ = src_str[0], src_str[1:]
        if t == MARSHAL:
            obj = marshal.loads(str_)
        elif t == PICKLE:
            obj = pickle.loads(str_)
        else:
            raise ValueError('String must contain a right type indicator.')
        return obj
            
    def _seek_writable_pos(self, map_handle):
        pos = 0
        while True:
            if pos + 4 <= self.store_file_size:
                size, = struct.unpack('I', map_handle[pos:pos+4])
                if size == 0:
                    return pos
                pos += (4 + size)
            else:
                return -1
        
        return -1
    
    def put_one(self, obj, force=False, commit=True):
        if self.stopped: return
        self.init()
        
        if isinstance(obj, str) and obj.strip() == '':
            return
        
        if not force and self.deduper is not None:
            prop = labelize(obj)
            if self.deduper.exist(prop):
                return
        if len(self.legal_files) == 0:
            self._generate_file()
            
        obj_str = self._stringfy(obj)
        # If no file has enough space
        if len(obj_str) + 4 > self.store_file_size:
            raise StoreNoSpaceForPut('No enouph space for this put.')
        
        with self.lock:
            m = self.map_handles[WRITE_ENTRANCE]
            pos = self._seek_writable_pos(m)
            size = pos + 4 + len(obj_str)
            
            if pos < 0 or size > self.store_file_size:
                m.flush()
                self._generate_file()
                pos = 0
                size = 4 + len(obj_str)
                m = self.map_handles[WRITE_ENTRANCE]
            
            m[:size] = m[:pos] + struct.pack('I', len(obj_str)) + obj_str
            if commit is True:
                m.flush()
                
        return obj
                    
    def put(self, objects, force=False, commit=True):
        if self.stopped: return
        self.init()
        
        if isinstance(objects, basestring) or not iterable(objects):
            return self.put_one(objects, force, commit)
            
        remains = []
        for obj in objects:
            result = self.put_one(obj, force, commit=False)
            if result is not None:
                remains.append(result)
        
        m = self.map_handles[WRITE_ENTRANCE]
        if len(remains) > 0 and m is not None:
            m.flush()
        return remains
                    
    def get_one(self, commit=True):
        if self.stopped: return
        self.init()
        
        m = self.map_handles[READ_ENTRANCE]
        while m is not None:
            with self.lock:
                size, = struct.unpack('I', m[:4])
                if size == 0:
                    self._destroy_file()
                    m = self.map_handles[READ_ENTRANCE]
                else:
                    obj = self._destringfy(m[4:4+size])
                    m[:] = m[4+size:] + '\x00' * (4+size)
                    if commit is True:
                        m.flush()
                    return obj
        
    def get(self, size=1):
        if size <= 1:
            return self.get_one()
        
        self.init()
        
        results = []
        for _ in range(size):
            obj = self.get_one()
            if obj is not None:
                results.append(obj)
        m = self.map_handles[READ_ENTRANCE]
        if m is not None:
            m.flush()
        return results
        
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.shutdown()
