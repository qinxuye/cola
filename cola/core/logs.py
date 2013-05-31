#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
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
    logger.addHandler(handler)
    
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    
    return logger