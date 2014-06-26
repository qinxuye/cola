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

Created on 2013-8-29

@author: Chine
'''

import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.bloomfilter import FileBloomFilter

BLOOM_FILETER_STATUS_FILENAME = 'dedup.bloomfilter.status'
MAP_DEDUP_STATUS_FILENAME = 'dedup.map.status'

class Deduper(object):
    def __init__(self, working_dir, *args, **kwargs):
        self.working_dir = working_dir
    
    def exist(self, key):
        raise NotImplementedError
    
    def shutdown(self):
        pass
    
class FileBloomFilterDeduper(Deduper):
    def __init__(self, working_dir, capacity, false_positive_rate=0.01):
        super(FileBloomFilterDeduper, self).__init__(working_dir)
        sync_file = os.path.join(self.working_dir, 
                                 BLOOM_FILETER_STATUS_FILENAME)
        
        self.filter = FileBloomFilter(sync_file, capacity,
                                      false_positive_rate=false_positive_rate)
        self.is_shutdown = False
        
    def exist(self, key):
        return self.filter.verify(key)
    
    def shutdown(self):
        if self.is_shutdown is True:
            return
        self.is_shutdown = True
        
        try:
            self.filter.sync()
        finally:
            self.filter.close()
            
    def __del__(self):
        self.shutdown()
        
class MapDeduper(Deduper):
    def __init__(self, working_dir, capacity, holder=dict):
        super(MapDeduper, self).__init__(working_dir)
        self.sync_file = os.path.join(self.working_dir,
                                      MAP_DEDUP_STATUS_FILENAME)
        if os.path.exists(self.sync_file) and \
            os.path.getsize(self.sync_file) > 0:
            with open(self.sync_file) as f:
                self.container = holder(pickle.load(f))
        else:
            self.container = holder()
            
        self.is_shutdown = False
        
    def exist(self, key):
        result = key in self.container
        if not result:
            self.container[key] = True
        return result
    
    def shutdown(self):
        if self.is_shutdown is True:
            return
        self.is_shutdown = True
        
        with open(self.sync_file, 'w') as f:
            pickle.dump(dict(self.container), f)
            
    def __del__(self):
        self.shutdown()