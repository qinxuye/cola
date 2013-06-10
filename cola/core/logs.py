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

Created on 2013-5-31

@author: Chine
'''

import logging

def get_logger(filename):
    logger = logging.getLogger('cola')
    logger.setLevel(logging.INFO)
    
    handler = logging.FileHandler(filename)
    formatter = logging.Formatter('%(asctime)s - %(module)s.%(funcName)s.%(lineno)d - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.ERROR)
    logger.addHandler(handler)
    
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    
    return logger