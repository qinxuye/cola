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
import cookielib

from cola.core.errors import DependencyNotInstalledError

class Opener(object):
    def open(self, url):
        raise NotImplementedError

class BuiltinOpener(Opener):
    def __init__(self, cookie_filename=None):
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
        urllib2.install_opener(self.opener)
    
    def open(self, url):
        return urllib2.urlopen(url).read()
    
class MechanizeOpener(Opener):
    def __init__(self, cookie_filename=None, user_agent=None):
        try:
            import mechanize
        except ImportError:
            raise DependencyNotInstalledError('mechanize')
        
        if user_agent is None:
            user_agent = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)'
        
        self.browser = mechanize.Browser()
        
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)
        self.browser.set_cookiejar(self.cj)
        self.browser.set_handle_equiv(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_handle_robots(False)
        self.browser.addheaders = [
            ('User-agent', user_agent)]
        
    def open(self, url, data=None):
        return self.browser.open(url, data=data).read()
    
    def browse_open(self, url, data=None):
        self.browser.open(url, data=data)
        return self.browser