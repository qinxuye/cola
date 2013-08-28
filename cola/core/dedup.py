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

from cola.core.bloomfilter import FileBloomFilter

class Deduper(object):
    def exist(self, key):
        raise NotImplementedError
    
class FileBloomFilterDeduper(Deduper):
    def __init__(self, sync_file, capacity):
        self.filter = FileBloomFilter(sync_file, capacity)
        
    def exist(self, key):
        return self.filter.verify(key)
    
    def __del__(self):
        try:
            self.filter.sync()
        finally:
            self.filter.close()