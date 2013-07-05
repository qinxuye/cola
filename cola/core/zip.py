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

Created on 2013-6-6

@author: Chine
'''

import os
import zipfile

class FixedZipFile(zipfile.ZipFile):
    '''
    Fixed for Python 2.6 when ZipFile doesn't support with statement
    '''
    
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.close()

class ZipHandler(object):
    
    @classmethod
    def compress(cls, zip_file, src_dir, type_filters=None):
        root_len = len(os.path.abspath(src_dir))
        dir_name = os.path.split(src_dir)[1].replace(' ', '_')
        
        with FixedZipFile(zip_file, 'w') as zf:
            if os.path.isfile(src_dir):
                zf.write(src_dir, dir_name)
            else:
                for root, _, files in os.walk(src_dir):
                    archive_root = os.path.abspath(root)[root_len:].strip(os.sep)
                    for f in files:
                        if type_filters is not None and '.' in f and \
                            f.rsplit('.', 1)[1] in type_filters:
                            continue
                        
                        full_path = os.path.join(root, f)
                        archive_name = os.path.join(dir_name, archive_root, f)
                        zf.write(full_path, archive_name)
                    
        return zip_file
    
    @classmethod
    def uncompress(cls, zip_file, dest_dir):
        dir_name = None
        with FixedZipFile(zip_file) as zf:
            for f in zf.namelist():
                zf.extract(f, dest_dir)
                if dir_name is None:
                    if '/' in f.strip('/'):
                        dir_name = f.strip('/').split('/')[0]
                    else:
                        dir_name = f.strip('/')
                    
        return os.path.join(dest_dir, dir_name)