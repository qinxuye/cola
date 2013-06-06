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
import threading
import mmap

class NodeExistsError(Exception): pass

class NodeNotSafetyShutdown(Exception): pass

class NodeNoSpaceForPut(Exception): pass

NODE_FILE_SIZE = 4 * 1024 * 1024 # single node store file must be less than 4M.

class Node(object):
    def __init__(self, dir_, size=NODE_FILE_SIZE, verify_exists_hook=None):
        self.lock = threading.Lock()
        self.NODE_FILE_SIZE = size
        self.verify_exists_hook = verify_exists_hook
        
        self.dir_ = dir_
        self.lock_file = os.path.join(dir_, 'lock')
        self.lock.acquire()
        try:
            if os.path.exists(self.lock_file):
                raise NodeExistsError('Directory is being used by another node.')
            else:
                open(self.lock_file, 'w').close()
        finally:
            self.lock.release()
            
        self.old_files = []
        self.map_files = []
        self.file_handles = {}
        self.map_handles = {}
        self.stopped = False
        self.check()
        self.map()
            
    def shutdown(self):
        if self.stopped: return
        self.stopped = True
        
        try:
            self.merge()
            
            for handle in self.map_handles.values():
                handle.close()
            for handle in self.file_handles.values():
                handle.close()
                
            # Move a store to an old one.
            for f in self.old_files:
                os.remove(f)
            for f in self.map_files:
                os.rename(f, f + '.old')
                
            if self.verify_exists_hook is not None:
                self.verify_exists_hook.sync()
                self.verify_exists_hook.close()
        finally:
            self.lock.acquire()
            try:
                os.remove(self.lock_file)
            finally:
                self.lock.release()
        
    def check(self):
        files = os.listdir(self.dir_)
        for fi in files:
            if fi == 'lock': continue
            
            file_path = os.path.join(self.dir_, fi)
            if not os.path.isfile(file_path) or \
                not fi.endswith('.old'):
                raise NodeNotSafetyShutdown('Node did not shutdown safety last time.')
            else:
                self.old_files.append(file_path)
                
        self.old_files = sorted(self.old_files, key=lambda k: int(os.path.split(k)[1].rsplit('.', 1)[0]))
        self.map_files = [f.rsplit('.', 1)[0] for f in self.old_files]
        
    def map(self):
        for (old, new) in zip(self.old_files, self.map_files):
            with open(old) as old_fp:
                fp = open(new, 'w+')
                self.file_handles[new] = fp
                content = old_fp.read()
                fp.write(content)
                fp.flush()
                
                if len(content) > 0:
                    m = mmap.mmap(fp.fileno(), self.NODE_FILE_SIZE)
                    self.map_handles[new] = m
                    
        if len(self.map_files) == 0:
            path = os.path.join(self.dir_, '1')
            self.map_files.append(path)
            self.file_handles[path] = open(path, 'w+')
                    
    def put(self, obj):
        if isinstance(obj, (tuple, list)):
            if self.verify_exists_hook is None:
                src_obj = obj
                obj = '\n'.join(obj) + '\n'
            else:
                src_obj = list()
                for itm in obj:
                    if not self.verify_exists_hook.verify(itm):
                        src_obj.append(itm)
                obj = '\n'.join(src_obj) + '\n'
        else:
            if self.verify_exists_hook is None:
                src_obj = obj
                obj = obj + '\n'
            else:
                if not self.verify_exists_hook.verify(obj):
                    src_obj = obj
                    obj = obj + '\n'
                else:
                    return ''
            
        # If no file has enough space
        if len(obj) > self.NODE_FILE_SIZE:
            raise NodeNoSpaceForPut('No enouph space for this put.')
        
        for f in self.map_files:
            # check if mmap created
            if f not in self.map_handles:
                fp = self.file_handles[f]
                fp.write(obj)
                fp.flush()
                
                m = mmap.mmap(fp.fileno(), self.NODE_FILE_SIZE)
                self.map_handles[f] = m
            else:
                m = self.map_handles[f]
                size = m.rfind('\n')
                new_size = size + 1 + len(obj)
                
                if new_size >= self.NODE_FILE_SIZE:
                    continue
                
                m[:new_size] = m[:size+1] + obj
                m.flush()
                
            return src_obj
        
        name = str(int(os.path.split(self.map_files[-1])[1]) + 1)
        path = os.path.join(self.dir_, name)
        self.map_files.append(path)
        fp = open(path, 'w+')
        self.file_handles[path] = fp
        fp.write(obj)
        fp.flush()
        self._add_handles(path)
        
        return src_obj
            
    def get(self):
        for m in self.map_handles.values():
            pos = m.find('\n')
            while pos >= 0:
                obj = m[:pos]
                m[:] = m[pos+1:] + '\x00' * (pos+1)
                m.flush()
                if len(obj.strip()) != 0:
                    return obj.strip()
                pos = m.find('\n')
        
    def _remove_handles(self, path):
        if path in self.map_handles:
            self.map_handles[path].close()
            del self.map_handles[path]
        if path in self.file_handles:
            self.file_handles[path].close()
            del self.file_handles[path]
            
    def _add_handles(self, path):
        if path not in self.file_handles:
            self.file_handles[path] = open(path, 'w+')
        if path not in self.map_handles and \
            os.path.getsize(path) > 0:
            self.map_handles[path] = mmap.mmap(
                self.file_handles[path].fileno(), self.NODE_FILE_SIZE)
        
    def merge(self):
        if len(self.map_files) > 1:
            for i in range(len(self.map_files)-1, 0, -1):
                f_path1 = self.map_files[i-1]
                f_path2 = self.map_files[i]
                m1 = self.map_handles[f_path1]
                m2 = self.map_handles[f_path2]
                pos1 = m1.rfind('\n')
                pos2 = m2.rfind('\n')
                
                if pos1 + pos2 + 2 < self.NODE_FILE_SIZE:
                    m1[:pos1+pos2+2] = m1[:pos1+1] + m2[:pos2+1]
                    m1.flush()
                            
                    self._remove_handles(f_path2)
                    self.map_files.remove(f_path2)
                    os.remove(f_path2)
                    
        for idx, f in enumerate(self.map_files):
            if not f.endswith(str(idx+1)):
                dir_ = os.path.dirname(f)
                self._remove_handles(f)
                self.map_files.remove(f)
                
                new_f = os.path.join(dir_, str(idx+1))
                os.rename(f, new_f)
                self.map_files.append(new_f)
                self._add_handles(new_f)
        self.map_files = sorted(self.map_files, key=lambda f: int(os.path.split(f)[1]))
        
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.shutdown()