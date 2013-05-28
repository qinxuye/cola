#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

class Bundle(object):
    '''
    Sometimes the target is all the urls about a user.
    Then the urls compose the bundle.
    So a bundle can generate several urls.
    '''
    
    def __init__(self, label):
        if not isinstance(label, str):
            raise ValueError("Bundle's label must a string.")
        self.label = label
        
    def urls(self):
        raise NotImplementedError
    
    def __str__(self):
        return self.label