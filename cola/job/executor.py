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
import traceback
import hashlib
import os
import random

from cola.core.unit import Bundle
from cola.core.errors import ConfigurationError, LoginFailure, \
                                ServerError, NetworkError, FetchBannedError
from cola.core.utils import Clock

ERROR_MSG_FILENAME = 'error.message'
ERROR_CONTENT_FILENAME = 'error.content.html'

DEFAULT_ERROR_SLEEP_SEC = 20
DEFAULT_ERROR_RETRY_TIMES = 15
DEFAULT_ERROR_IGNORE = False

class BundleInterrupt(Exception): pass

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
    
    def login(self, random=False):
        if self.is_local:
            if not self._login(shuffle=random):
                self.save()
                raise
        else:
            while not self.stopped.is_set():
                if self._login(shuffle=random):
                    break
                if self.stopped.wait(5):
                    break
    
    def _login(self, shuffle=False):
        if self.job_desc.login_hook is not None:
            if 'login' not in self.settings.job or \
                not isinstance(self.settings.job.login, list):
                raise ConfigurationError('If login_hook set, config files must contains `login`')
            
            kws = self.settings.job.login
            if shuffle:
                random.shuffle(kws)
            else:
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
        self.login(random=True)
        
    def _pack_error(self, url, msg, error, content=None,
                    force_pack_content=False):
        filename = hashlib.md5(str(url)).hexdigest()
        path = os.path.join(self.dir_, 'errors', filename)
        if not os.path.exists(path):
            os.makedirs(path)
            
        msg_filename = os.path.join(path, ERROR_MSG_FILENAME)
        with open(msg_filename, 'w') as f:
            f.write(msg+'\n')
            traceback.print_exc(file=f)
            
        content_filename = os.path.join(path, 
                                        ERROR_CONTENT_FILENAME)
        if force_pack_content is True and content is None:
            try:
                content = self.opener.open(url)
            except ServerError, e:
                content = e.read()
            except:
                return
        if content is not None:
            with open(content_filename, 'w') as f:
                f.write(content)
                
    def _get_handle_error_params(self, e):
        params = self.settings.job.error
        retries = DEFAULT_ERROR_RETRY_TIMES
        span = DEFAULT_ERROR_SLEEP_SEC
        ignore = DEFAULT_ERROR_IGNORE
        
        if isinstance(e, ServerError):
            retries = params.server.retries
            span = params.server.span
            ignore = params.server.ignore
        elif isinstance(e, NetworkError):
            retries = params.network.retries
            span = params.network.span
            ignore = params.network.ignore
        
        return retries, span, ignore
    
    def _handle_fetch_banned(self):
        # need to fix
        raise

class UrlExecutor(Executor):
    def execute(self, url):
        pass

class BundleExecutor(Executor):
    def _parse(self, parser_cls, options, bundle, url):
        if hasattr(self.opener, 'content'):
            del self.opener.content
            
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
        
    def _log_error(self, bundle, url, e):
        if self.logger:
            self.logger.exception(e)
        bundle.error_times = getattr(bundle, 'error_times', 0) + 1
        bundle.error_url = url

    def _handle_error(self, bundle, url, e, pack=True):
        # pause clock
        self.clock.pause()
        
        try:
            self._log_error(bundle, url, e)
            retries, span, ignore = self._get_handle_error_params(e)
            if bundle.error_times <= retries:
                bundle.current_urls.insert(0, url)
                self.stopped.wait(span)
                return
            
            if pack:
                content = getattr(self.opener, 'content', None)
                if content is None and isinstance(e, ServerError):
                    content = e.read()
                msg = 'Error when handle bundle: %s, url: %s' % (str(bundle), 
                                                                 str(url))
                self._pack_error(url, msg, e, content)
                
            if ignore:
                bundle.error_urls.append(url)
                return
            else:
                bundle.current_urls.insert(0, url)
                raise BundleInterrupt
        finally:
            self.clock.resume()

    def _clear_error(self, bundle):
        if hasattr(bundle, 'error_url'):
            del bundle.error_url
        if hasattr(bundle, 'error_times'):
            del bundle.error_times
        
    def _parse_with_process_exception(self, parser_cls, options, 
                                      bundle, url):
        try:
            res = self._parse(parser_cls, options, bundle, url)
            self._clear_error(bundle)
            return res
        except LoginFailure, e:
            self._handle_error(bundle, url, e)
            self.clear_and_relogin()
        except FetchBannedError, e:
            self._handle_error(bundle, url, e)
        except ServerError, e:
            self._handle_error(bundle, url, e)
        except NetworkError, e:
            self._handle_error(bundle, url, e, pack=False)
        except Exception, e:
            self._handle_error(bundle, url, e)
            
        return [], []
        
    def execute(self, bundle, max_sec):
        self.clock = Clock()
        time_exceed = lambda: self.clock.clock() >= max_sec
        
        bundle.current_urls = getattr(bundle, 'current_urls', []) \
                                    or bundle.urls()
        bundle.current_urls.extend(getattr(bundle, 'error_urls', []))
        
        while len(bundle.current_urls) > 0 and not time_exceed():
            url = bundle.current_urls.pop(0)
            if self.info_logger:
                self.info_logger.info('get %s url: %s' % 
                                      (bundle.label, url))
            
            parser_cls, options = self.job.url_patterns.get_parser(url, options=True)
            if parser_cls is not None:
                self.loader._require_budget()
                self.loader.pages_size += 1
                
                next_urls, bundles = self._parse_with_process_exception(
                    parser_cls, options, bundle, url)
                next_urls = list(self.job_desc.url_patterns.matches(next_urls))
                next_urls.extend(bundle.current_urls)
                bundle.current_urls = next_urls
                if bundles:
                    self.mq.put(bundles)
                if hasattr(self.opener, 'close'):
                    self.opener.close()
        
        if len(bundle.current_urls) == 0:
            if self.job_desc.settings.job.inc == True:
                self.mq.put_inc(bundle)
        else:
            return bundle