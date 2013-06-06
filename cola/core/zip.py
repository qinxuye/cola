#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-6

@author: Chine
'''

import os
import zipfile

class ZipHandler(object):
    
    @classmethod
    def compress(cls, zip_file, src_dir):
        root_len = len(os.path.abspath(src_dir))
        dir_name = os.path.split(src_dir)[1].replace(' ', '_')
        
        with zipfile.ZipFile(zip_file, 'w') as zf:
            for root, _, files in os.walk(src_dir):
                archive_root = os.path.abspath(root)[root_len:].strip(os.sep)
                for f in files:
                    full_path = os.path.join(root, f)
                    archive_name = os.path.join(dir_name, archive_root, f)
                    zf.write(full_path, archive_name)
                    
        return zip_file
    
    @classmethod
    def uncompress(cls, zip_file, dest_dir):
        dir_name = None
        with zipfile.ZipFile(zip_file) as zf:
            for f in zf.namelist():
                zf.extract(f, dest_dir)
                if dir_name is None and '/' not in f.strip('/'):
                    dir_name = f.strip('/')
                    
        return os.path.join(dest_dir, dir_name)