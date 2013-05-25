#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-25

@author: Chine
'''

import os

from cola.core.config import Config

conf_base_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf')
conf_path = lambda name: os.path.join(conf_base_path, name)

main_conf = Config(conf_path('main.yaml'))