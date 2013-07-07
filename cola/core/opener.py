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
import gzip

from cola.core.errors import DependencyNotInstalledError

class Opener(object):
    def open(self, url):
        raise NotImplementedError
    
    def ungzip(self, fileobj):
        gz = gzip.GzipFile(fileobj=fileobj, mode='rb')
        try:
            return gz.read()
        finally:
            gz.close()

class BuiltinOpener(Opener):
    def __init__(self, cookie_filename=None):
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
        urllib2.install_opener(self.opener)
    
    def open(self, url):
        resp = urllib2.urlopen(url)
        is_gzip = resp.headers.dict.get('content-encoding') == 'gzip'
        if is_gzip:
            return self.ungzip(resp)
        return resp.read()
        
    
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
        self.browser.set_handle_gzip(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_handle_robots(False)
        self.browser.addheaders = [
            ('User-agent', user_agent)]
        
    def open(self, url, data=None):
        # check if gzip by
        # br.response().info().dict.get('content-encoding') == 'gzip'
        # experimently add `self.br.set_handle_gzip(True)` to handle
        return self.browser.open(url, data=data).read()
    
    def browse_open(self, url, data=None):
        self.browser.open(url, data=data)
        return self.browser
    
    def close(self):
        self.browser.response().close()
        self.browser.clear_history()
    
class SpynnerOpener(Opener):
    def __init__(self, user_agent=None, **kwargs):
        try:
            import spynner
        except ImportError:
            raise DependencyNotInstalledError('spynner')
        
        if user_agent is None:
            user_agent = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)'
        
        self.br = spynner.Browser(user_agent=user_agent, **kwargs)
        
    def spynner_open(self, url, data=None, headers=None, method='GET', 
                     wait_for_text=None, wait_for_selector=None, tries=None):
        try:
            from PyQt4.QtNetwork import QNetworkAccessManager
        except ImportError:
            raise DependencyNotInstalledError('PyQt4')
        
        if wait_for_text is not None:
            def wait_callback(br):
                return wait_for_text in br.html
        elif wait_for_selector is not None:
            def wait_callback(br):
                return not br.webframe.findFirstElement(wait_for_selector).isNull()
        else:
            wait_callback = None
        
        operation = QNetworkAccessManager.GetOperation
        if method == 'POST':
            operation = QNetworkAccessManager.PostOperation
        self.br.load(url, wait_callback=wait_callback, tries=tries, 
                     operation=operation, body=data, headers=headers)
        
        return self.br
        
    def open(self, url, data=None, headers=None, method='GET', 
             wait_for_text=None, wait_for_selector=None, tries=None):
        br = self.spynner_open(url, data=data, headers=headers, method=method, 
                               wait_for_text=wait_for_text, tries=tries)
        return br.contents
    
    def wait_for_selector(self, selector, **kwargs):
        self.br.wait_for_content(
            lambda br: not br.webframe.findFirstElement(selector).isNull(), 
            **kwargs)