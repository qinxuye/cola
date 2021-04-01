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
import time

from cola.core.unit import Bundle
from cola.core.errors import ConfigurationError, LoginFailure, \
                                ServerError, NetworkError, FetchBannedError, \
                                DependencyNotInstalledError
from cola.core.utils import Clock, get_ip
from cola.settings import ReadOnlySettings

try:
    from collections import OrderedDict
except ImportError:
    try:
        from ordereddict import OrderedDict
    except ImportError:
        raise DependencyNotInstalledError('ordereddict')

ERROR_MSG_FILENAME = 'error.message'
ERROR_CONTENT_FILENAME = 'error.content.html'

DEFAULT_ERROR_SLEEP_SEC = 20
DEFAULT_ERROR_RETRY_TIMES = 15
DEFAULT_ERROR_IGNORE = False
DEFAULT_SPEEED_REQUIRE_SIZE = 5

DEFAULT_OPENER_TIMEOUT = 20

class UnitRetryFailed(Exception): pass

class ExecutorCounter(object):
    def __init__(self, executor):
        self.executor = executor
        self.counter = executor.counter_client
        
    def inc(self, item, value=1):
        self.counter.local_inc(self.executor.ip, self.executor.id_, 
                               item, val=value)
        self.counter.global_inc(item, val=value)

class Executor(object):
    def __init__(self, id_, job_desc, mq, 
                 working_dir, stopped, nonsuspend, 
                 budget_client, speed_client, counter_client, 
                 is_local=False, env=None, logger=None):
        self.id_ = id_
        self.job_desc = job_desc
        self.opener = job_desc.opener_cls(timeout=DEFAULT_OPENER_TIMEOUT)
        self.mq = mq
        self.dir_ = working_dir
        self.settings = job_desc.settings
        
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        
        self.budget_client = budget_client
        self.speed_client = speed_client
        self.counter_client = counter_client
        
        if env is None:
            env = {}
        self.env = env
        self.is_local = is_local
        self.ip = env.get('ip') or get_ip()
        self.logger = logger
        
        self._configure_proxy()
        
        self.processing_inc = False
        
        # used for tracking if banned
        self.is_normal = True
        self.normal_start = time.time()
        self.normal_pages = 0
        self.banned_start = None
        
        self.banned_handler_idx = -1
        self.handle_banned_by_proxy = False
        self._configure_banned_handler()
        self.banned_handler_size = len(self.banned_handlers)
        
    def _configure_proxy(self):
        if not hasattr(self.opener, 'add_proxy'):
            return
        
        if self.settings.job.has('proxies'):
            proxies = self.settings.job.proxies
            try:
                for p in proxies:
                    proxy_type = p.type if p.has('type') else 'all'
                    if p.has('addr'):
                        self.opener.add_proxy(p.addr, 
                            proxy_type=proxy_type,
                            user=p.user if p.has('user') else None,
                            password=p.password if p.has('password') else None)
            except TypeError:
                return
            
    def _configure_banned_handler(self):
        if self.settings.job.has('banned_handlers'):
            handlers = self.settings.job.banned_handlers
            self.banned_handlers = []
            try:
                for h in handlers:
                    if not h.has('action'):
                        return
                    
                    if h.action == 'relogin':
                        self.banned_handlers.append(self.clear_and_relogin)
                    elif h.action == 'proxy':
                        if not h.has('addr') or not hasattr(self.opener, 'add_proxy'):
                            continue
                        proxy_type = h.type if h.has('type') else 'all'
                        def proxy_handler():
                            self.handle_banned_by_proxy = True
                            self.opener.add_proxy(h.addr, 
                                                  proxy_type=proxy_type,
                                                  user=h.user if h.has('user') else None,
                                                  password=h.password if h.has('password') else None)
                        self.banned_handlers.append(proxy_handler)
                    elif h.action == 'clear_proxy':
                        if hasattr(self.opener, 'remove_proxy'):
                            self.banned_handlers.append(self.opener.remove_proxy)
                        
            except TypeError:
                return
        
    def execute(self):
        raise NotImplementedError
    
    def login(self, random=False):
        if self.is_local:
            if not self._login(shuffle=random):
                raise LoginFailure('login failed')
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
                idx = self.id_ % len(kws)
                kws = kws[idx:] + kws[:idx]
            
            for kw in kws:
                login_result = self.job_desc.login_hook(self.opener, **kw)
                if isinstance(login_result, tuple) and len(login_result) == 2 and \
                    not login_result[0]:
                    if self.logger:
                        self.logger.error('instance %s, login fail, reason: %s' % \
                                          (self.id_, login_result[1]))
                    continue
                elif not login_result:
                    if self.logger:
                        self.logger.error('instance %s: login fail' % self.task_id)
                    continue
                return login_result
            return False
        
        return True
    
    def clear_and_relogin(self):
        self.opener = self.job_desc.opener_cls(timeout=DEFAULT_OPENER_TIMEOUT)
        self.login(random=True)
        
    def _pack_error(self, url, msg, error, content=None,
                    force_pack_content=False):
        filename = hashlib.md5(str(url)).hexdigest()
        path = os.path.join(self.dir_, 'errors', filename)
        if not os.path.exists(path):
            os.makedirs(path)
            
        msg_filename = os.path.join(path, ERROR_MSG_FILENAME)
        with open(msg_filename, 'w') as f:
            f.write(msg + '\n')
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
        self.banned_handler_idx += 1
        index = self.banned_handler_idx % self.banned_handler_size
        self.banned_handlers[index]()
        
    def _handle_proxy_network_error(self, e):
        if self.handle_banned_by_proxy and \
            isinstance(e, NetworkError) and \
                hasattr(self.opener, 'remove_proxy'):
            self.opener.remove_proxy()
            self.handle_banned_by_proxy = False
        
    def _finish(self, unit):
        if self.logger:
            self.logger.info('Finish %s' % str(unit))
        if self.processing_inc:
            self.counter_client.local_inc(self.ip, self.id_,
                                          'inc_finishes', 1)
            self.counter_client.global_inc('inc_finishes', 1)
        else:
            self.budget_client.finish()
            self.counter_client.local_inc(self.ip, self.id_,
                                          'finishes', 1)
            self.counter_client.global_inc('finishes', 1)
        
    def _error(self):
        if self.processing_inc:
            self.counter_client.local_inc(self.ip, self.id_, 
                                          'inc_errors', 1)
            self.counter_client.global_inc('inc_errors', 1)
        else: 
            self.budget_client.error()
            self.counter_client.local_inc(self.ip, self.id_, 
                                          'errors', 1)
            self.counter_client.global_inc('errors', 1)
        
    def _got_banned(self):
        if self.is_normal:
            self.is_normal = False
            self.normal_pages = 0
            curr = time.time()
            kw = {'normal_start': self.normal_start, 
                  'normal_end': curr,
                  'normal_pages': self.normal_pages}
            self.banned_start = curr
            self.counter_client.multi_local_acc(self.ip, self.id_, **kw) 
            
    def _recover_normal(self):
        if not self.is_normal:
            self.is_normal = True
            curr = time.time()
            kw = {'banned_start': self.banned_start, 
                  'banned_end': curr}
            self.normal_start = curr
            self.counter_client.multi_local_acc(self.ip, self.id_, **kw)
        self.normal_pages += 1

class UrlExecutor(Executor):
    def __init__(self, *args, **kwargs):
        super(UrlExecutor, self).__init__(*args, **kwargs)
        self.budges = 0
    
    def _parse(self, parser_cls, options, url):
        if hasattr(self, 'content'):
            del self.opener.content
            
        res = parser_cls(self.opener, url, 
                         logger=self.logger, 
                         counter=ExecutorCounter(self),
                         settings=ReadOnlySettings(self.settings),
                         **options).parse()
        if res:
            return list(res)
        else:
            return list()
    
    def _log_error(self, url, e):
        if self.logger:
            self.logger.error('Error when handle url: %s' % (str(url)))
            self.logger.exception(e)

        url.error_times = getattr(url, 'error_times', 0) + 1
            
        self.counter_client.local_inc(self.ip, self.id_, 
                                      'error_urls', 1)
        self.counter_client.global_inc('error_urls', 1)
        
    def _handle_error(self, url, e, pack=True):
        if self.job_desc.error_handler:
            self.job_desc.error_handler.handle(e, url)
        
        self._log_error(url, e)
        retries, span, ignore = self._get_handle_error_params(e)
        if url.error_times <= retries:
            self.stopped.wait(span)
            return
        
        self._handle_proxy_network_error(e)
        
        if pack:
            content = self.opener.read()
            if content is None and isinstance(e, ServerError):
                content = e.read()
            msg = 'Error when handle url: %s' % str(url)
            self._pack_error(url, msg, e, content)
            
        if not ignore:
            self._error()
            raise UnitRetryFailed


    def _clear_error(self, url):
        if hasattr(url, 'error_times'):
            del url.error_times
            
    def _parse_with_process_exception(self, parser_cls, options, url):
        try:
            clock = Clock()
            
            res = self._parse(parser_cls, options, str(url))
            
            t = clock.clock()
            kw = {'pages': 1, 'secs': t}
            self.counter_client.multi_local_inc(self.ip, self.id_, **kw)
            self.counter_client.multi_global_inc(**kw)

            self._clear_error(url)
            self._recover_normal()
            
            return res
        except LoginFailure, e:
            self._handle_error(url, e)
            self.clear_and_relogin()
        except FetchBannedError, e:
            self._handle_error(url, e)
            self._got_banned()
            self._handle_fetch_banned()
        except ServerError, e:
            self._handle_error(url, e)
        except NetworkError, e:
            self._handle_error(url, e, pack=False)
        except Exception, e:
            self._handle_error(url, e)
            
        return [url,]
    
    def execute(self, url, is_inc=False):
        failed = False
        
        while not self.nonsuspend.wait(5):
            continue
        if self.stopped.is_set():
            return
        
        if self.logger:
            self.logger.info('get url: %s' % str(url))
        
        self.processing_inc = is_inc
        rates = 0
        span = 0.0
        parser_cls, options = self.job_desc.url_patterns.get_parser(url, options=True)
        if parser_cls is not None:
            if rates == 0:
                rates, span = self.speed_client.require(DEFAULT_SPEEED_REQUIRE_SIZE)
            if rates == 0:
                if self.stopped.wait(5):
                    return
            rates -= 1
            
            try:
                next_urls = self._parse_with_process_exception(parser_cls, options, url)
                next_urls = list(self.job_desc.url_patterns.matches(next_urls))
                if next_urls:
                    self.mq.put(next_urls)
                    # inc budget if auto budget enabled
                    if self.settings.job.size == 'auto':
                        inc_budgets = len(next_urls)
                        if inc_budgets > 0:
                            self.budget_client.inc_budgets(inc_budgets)
                if hasattr(self.opener, 'close'):
                    self.opener.close()
                    
                self.stopped.wait(span)
            except UnitRetryFailed:
                failed = True
        
        if self.settings.job.inc is True:
            self.mq.put_inc(url)
        if not failed:
            self._finish(url)
        if failed:
            return url

class BundleExecutor(Executor):
    def __init__(self, *args, **kwargs):
        super(BundleExecutor, self).__init__(*args, **kwargs)
        self.shuffle_urls = self.settings.job.shuffle
    
    def _parse(self, parser_cls, options, bundle, url):
        if hasattr(self.opener, 'content'):
            del self.opener.content
            
        res = parser_cls(self.opener, url, bundle=bundle,
                         logger=self.logger, counter=ExecutorCounter(self),
                         settings=ReadOnlySettings(self.settings),
                         **options).parse()
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
            self.logger.error('Error when handle bundle: %s, url: %s' % (str(bundle), str(url)))
            self.logger.exception(e)
        if url == getattr(bundle, 'error_url', None):
            bundle.error_times = getattr(bundle, 'error_times', 0) + 1
        else:
            bundle.error_times = 0
            bundle.error_url = url
            
        self.counter_client.local_inc(self.ip, self.id_, 
                                      'error_urls', 1)
        self.counter_client.global_inc('error_urls', 1)

    def _handle_error(self, bundle, url, e, pack=True):
        # pause clock
        self.clock.pause()
        
        if self.job_desc.error_handler:
            self.job_desc.error_handler.handle(e, bundle)
        
        try:
            self._log_error(bundle, url, e)
            retries, span, ignore = self._get_handle_error_params(e)
            if retries < 0 or bundle.error_times <= retries:
                if url not in bundle.current_urls:
                    bundle.current_urls.insert(0, url)
                self.stopped.wait(span)
                return
            
            self._handle_proxy_network_error(e)

            if pack:
                content = self.opener.read()
                if content is None and isinstance(e, ServerError):
                    content = e.read()
                msg = 'Error when handle bundle: %s, url: %s' % (str(bundle), 
                                                                 str(url))
                self._pack_error(url, msg, e, content)
                
            if ignore:
                bundle.error_urls.append(url)
                # dec budget if auto budget enabled
                if self.settings.job.size == 'auto':
                    self.budget_client.dec_budgets(1)
                return
            else:
                bundle.current_urls.insert(0, url)
                self._error()
                raise UnitRetryFailed
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
            clock = Clock()
            
            res = self._parse(parser_cls, options, bundle, url)
            
            t = clock.clock()
            kw = {'pages': 1, 'secs': t}
            self.counter_client.multi_local_inc(self.ip, self.id_, **kw)
            self.counter_client.multi_global_inc(**kw)
            

            self._clear_error(bundle)
            self._recover_normal()
            
            return res
        except LoginFailure, e:
            self._handle_error(bundle, url, e)
            self.clear_and_relogin()
        except FetchBannedError, e:
            self._handle_error(bundle, url, e)
            self._got_banned()
            self._handle_fetch_banned()
        except ServerError, e:
            self._handle_error(bundle, url, e)
        except NetworkError, e:
            self._handle_error(bundle, url, e, pack=False)
        except Exception, e:
            self._handle_error(bundle, url, e)
            
        return [url,], []
        
    def execute(self, bundle, max_sec, is_inc=False):
        failed = False
        self.clock = Clock()
        time_exceed = lambda: self.clock.clock() >= max_sec
        
        self.processing_inc = is_inc
        bundle.current_urls = getattr(bundle, 'current_urls', []) \
                                    or bundle.urls()
        bundle.current_urls.extend(getattr(bundle, 'error_urls', []))
        
        while not self.stopped.is_set() and len(bundle.current_urls) > 0 \
            and not time_exceed():
            
            while not self.nonsuspend.wait(5):
                continue
            if self.stopped.is_set():
                break
            
            url = bundle.current_urls.pop(0)
            if self.logger:
                self.logger.debug('get %s url: %s' % (bundle.label, url))
            
            rates = 0
            span = 0.0
            parser_cls, options = self.job_desc.url_patterns.get_parser(url, 
                                                                        options=True)
            if parser_cls is not None:
                if rates == 0:
                    rates, span = self.speed_client.require(DEFAULT_SPEEED_REQUIRE_SIZE)
                if rates == 0:
                    if self.stopped.wait(5):
                        break
                rates -= 1
                
                try:
                    next_urls, bundles = self._parse_with_process_exception(parser_cls, options, bundle, url)
                    next_urls = list(self.job_desc.url_patterns.matches(next_urls))
                    next_urls.extend(bundle.current_urls)
                    if self.shuffle_urls:
                        if len(next_urls) > 0 and next_urls[0] == url:
                            next_urls = next_urls[1:]
                            random.shuffle(next_urls)
                            next_urls.insert(0, url)
                        else:
                            random.shuffle(next_urls)
                    bundle.current_urls = OrderedDict.fromkeys(next_urls).keys()
                    
                    if bundles:
                        self.mq.put(bundles)
                        # inc budget if auto budget enabled
                        if self.settings.job.size == 'auto':
                            inc_budgets = len(bundles)
                            if inc_budgets > 0:
                                self.budget_client.inc_budgets(inc_budgets)

                    if hasattr(self.opener, 'close'):
                        self.opener.close()
                        
                    if self.stopped.wait(span):
                        break
                except UnitRetryFailed:
                    failed = True
                    break
        
        if len(bundle.current_urls) == 0 or failed:
            if self.settings.job.inc == True:
                self.mq.put_inc(bundle)
                
            if not failed:
                self._finish(bundle)
            else:
                return bundle
        else:
            return bundle