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

Created on 2013-5-17

@author: Chine
'''

import urllib2

class DependencyNotInstalledError(Exception):
    def __init__(self, dep):
        self.dep = dep
        
    def __str__(self):
        return 'Error because lacking of dependency: %s' % self.dep
    
class ConfigurationError(Exception): pass

class LoginFailure(Exception): pass

class FetchBannedError(Exception): pass

ServerError = urllib2.HTTPError
NetworkError = urllib2.URLError