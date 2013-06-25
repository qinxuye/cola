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
        self.force = False
        
    def urls(self):
        raise NotImplementedError
    
    def __str__(self):
        return self.label