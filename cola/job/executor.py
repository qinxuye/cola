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

Created on 2014-5-17

@author: chine
'''

import types
# import urllib2

from cola.core.unit import Bundle
from cola.core.errors import ConfigurationError, LoginFailure
from cola.core.utils import Clock

class Executor(object):
    def __init__(self, job_desc, mq,  
                 working_dir, stopped, nonsuspend, 
                 budget_client, speed_client, counter_client, 
                 logger=None, info_logger=None):
        self.job_desc = job_desc
        self.opener = job_desc.opener_cls()
        self.mq = mq
        self.dir_ = working_dir
        self.settings = job_desc.settings
        
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        
        self.budge_client = budget_client
        self.speed_client = speed_client
        self.counter_client = counter_client
        
        self.logger = logger
        self.info_logger = info_logger
        
    def execute(self):
        raise NotImplementedError
    
    def login(self):
        if self.is_local:
            if not self._login():
                self.save()
                raise
        else:
            while not self.stopped.is_set():
                if self._login():
                    break
                if self.stopped.wait(5):
                    break
    
    def _login(self):
        if self.job_desc.login_hook is not None:
            if 'login' not in self.settings.job or \
                not isinstance(self.settings.job.login, list):
                raise ConfigurationError('If login_hook set, config files must contains `login`')
            
            kws = self.settings.job.login
            idx = self.task_id % len(kws)
            kws = kws[idx:] + kws[:idx]
            
            for kw in kws:
                login_result = self.job_desc.login_hook(self.opener, **kw)
                if isinstance(login_result, tuple) and len(login_result) == 2 and \
                    not login_result[0]:
                    if self.logger:
                        self.logger.error('instance %s, login fail, reason: %s' % \
                                          (self.task_id, login_result[1]))
                    continue
                elif not login_result:
                    if self.logger:
                        self.logger.error('instance %s: login fail' % self.task_id)
                    continue
                return login_result
            return False
        
        return True
    
    def clear_and_relogin(self):
        self.opener = self.job_desc.opener_cls()
        self._login_wrapper()

class UrlExecutor(Executor):
    def execute(self, url):
        pass

class BundleExecutor(Executor):
    def _parse(self, parser_cls, options, bundle, url):
        res = parser_cls(self.opener, url, bundle=bundle,
                         logger=self.logger, **options).parse()
        if isinstance(res, tuple):
            return res
        elif isinstance(res, types.GeneratorType):
            next_urls, bundles = [], []
            for obj in res:
                if isinstance(obj, Bundle):
                    bundles.append(obj)
                else:
                    next_urls.append(obj)
            return next_urls, bundles
        else:
            return [], []
        
    def _parse_with_process_exception(self, parser_cls, options, 
                                      bundle, url):
        try:
            self._parse(parser_cls, options, bundle, url)
        except LoginFailure, e:
            if not self.loader._login(self.opener):
                self._error(bundle, url, e)
            elif url is not None:
                bundle.current_urls.insert(0, url)
        except Exception, e:
            if self.logger is not None and url is not None:
                self.logger.error('Error when fetch url: %s' % url)
            self._error(bundle, url, e)
        
    def execute(self, bundle, max_sec):
        clock = Clock()
        time_exceed = lambda: clock.clock() >= max_sec
        
        bundle.current_urls = getattr(bundle, 'current_urls', []) \
                                    or bundle.urls()
        
        while len(bundle.current_urls) > 0 and not time_exceed():
            url = bundle.current_urls[0]
            self.loader.info_logger.info('get %s url: %s' % 
                                         (bundle.label, url))
            
            parser_cls, options = self.job.url_patterns.get_parser(url, options=True)
            if parser_cls is not None:
                self.loader._require_budget()
                self.loader.pages_size += 1
                
                next_urls, bundles = self._parse(parser_cls, options, bundle, url)
                next_urls = list(self.job_desc.url_patterns.matches(next_urls))
                next_urls.extend(bundle.current_urls)
                bundle.current_urls = next_urls
                if bundles:
                    self.mq.put(bundles)
                if hasattr(self.opener, 'close'):
                    self.opener.close()
            
            # if finish normally, remove from the list
            bundle.current_urls.remove(url)
        
        if len(bundle.current_urls) == 0:
            if self.job_desc.settings.job.inc == True:
                self.mq.put_inc(bundle)
        else:
            return bundle