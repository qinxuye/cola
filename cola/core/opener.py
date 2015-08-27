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
import socket

from cola.core.errors import DependencyNotInstalledError

class Opener(object):
    def open(self, url):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError
    
    def ungzip(self, fileobj):
        gz = gzip.GzipFile(fileobj=fileobj, mode='rb')
        try:
            return gz.read()
        finally:
            gz.close()

class BuiltinOpener(Opener):
    def __init__(self, cookie_filename=None, timeout=None, **kwargs):
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        self._build_opener()
        urllib2.install_opener(self.opener)
        
        if timeout is None:
            self._default_timeout = socket._GLOBAL_DEFAULT_TIMEOUT
        else:
            self._default_timeout = timeout
    
    def _build_opener(self):
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
    
    def open(self, url, data=None, timeout=None):
        if timeout is None:
            timeout = self._default_timeout
            
        resp = urllib2.urlopen(url, data=data, timeout=timeout)
        is_gzip = resp.headers.dict.get('content-encoding') == 'gzip'
        if is_gzip:
            return self.ungzip(resp)
        self.content = resp.read()
        return self.content

    def read(self):
        return self.content if hasattr(self, 'content') else None
    
    def add_proxy(self, addr, proxy_type='all',
                  user=None, password=None):
        if proxy_type == 'all':
            self.proxies = {'http': addr, 'https': addr, 'ftp': addr}
        else:
            self.proxies[proxy_type] = addr
        proxy_handler = urllib2.ProxyHandler(self.proxies)
        self._build_opener()
        self.opener.add_handler(proxy_handler)
        
        if user and password:
            pwd_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
            pwd_manager.add_password(None, addr, user, password)
            proxy_auth_handler = urllib2.ProxyBasicAuthHandler(pwd_manager)
            self.opener.add_handler(proxy_auth_handler)
        
        urllib2.install_opener(self.opener)
    
    def remove_proxy(self):
        self._build_opener()
        urllib2.install_opener(self.opener)
    
class MechanizeOpener(Opener):
    def __init__(self, cookie_filename=None, user_agent=None, timeout=None, **kwargs):
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
        
        if timeout is None:
            self._default_timout = mechanize._sockettimeout._GLOBAL_DEFAULT_TIMEOUT
        else:
            self._default_timout = timeout
            
    def set_default_timeout(self, timeout):
        self._default_timout = timeout
        
    def open(self, url, data=None, timeout=None):
        # check if gzip by
        # br.response().info().dict.get('content-encoding') == 'gzip'
        # experimently add `self.br.set_handle_gzip(True)` to handle
        if timeout is None:
            timeout = self._default_timout
        self.content = self.browser.open(url, data=data, timeout=timeout).read()
        return self.content
    
    def add_proxy(self, addr, proxy_type='all', 
                  user=None, password=None):
        if proxy_type == 'all':
            self.proxies = {'http': addr, 'https': addr, 'ftp': addr}
        else:
            self.proxies[proxy_type] = addr
        self.browser.set_proxies(proxies=self.proxies)
        if user and password:
            self.browser.add_proxy_password(user, password)
    
    def remove_proxy(self):
        self.browser.set_proxies({})
        self.proxies = {}
    
    def browse_open(self, url, data=None, timeout=None):
        if timeout is None:
            timeout = self._default_timout
        self.browser.open(url, data=data, timeout=timeout)
        return self.browser

    def read(self):
        if hasattr(self, 'content'):
            return self.content
        elif self.browser.response() is not None:
            return self.browser.response().read()
    
    def close(self):
        if hasattr(self, 'content'):
            del self.content
        resp = self.browser.response()
        if resp is not None:
            resp.close()
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
        self.content = br.contents
        return self.content

    def read(self):
        return self.content if hasattr(self, 'content') else self.br.contents
    
    def wait_for_selector(self, selector, **kwargs):
        self.br.wait_for_content(
            lambda br: not br.webframe.findFirstElement(selector).isNull(), 
            **kwargs)