#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
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
        
    def open(self, url):
        return self.browser.open(url).read()
    
    def browse_open(self, url):
        self.browser.open(url)
        return self.browser