#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-17

@author: Chine
'''

class DependencyNotInstalledError(Exception):
    def __init__(self, dep):
        self.dep = dep
        
    def __str__(self):
        return 'Error because lacking of dependency: %s' % self.dep
    
class ConfigurationError(Exception): pass