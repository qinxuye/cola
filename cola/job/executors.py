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

Created on 2014-4-20

@author: chine
'''

import time
import os
import multiprocessing
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.errors import LoginFailure

SERIAL_FILENAME = 'executor.serial'

MAX_RUNNING_SECONDS = 10 * 60 # max seconds for a unit to run

EMPTY_TIMES = 3
MAX_ERROR_TIMES = 5

class Executor(object):
    def __init__(self, working_dir, job_loader):
        self.dir_ = working_dir
        self.loader = job_loader
        self.mq = self.loader.mq
        self.n_priorities = self.mq.n_priorities
        self.serial_file = os.path.join(self.dir_, SERIAL_FILENAME)
        
        self.priorities_secs = tuple(
            [MAX_RUNNING_SECONDS/(2**i) for i in range(self.n_priorities+1)])
        self.priorities_objs = [[]] * (self.n_priorities+1)
        
        self.error_times = 0
        
        self.stopped = False
    
    @classmethod
    def load(self, serial_file, job_loader):
        if os.path.exists(serial_file):
            with open(serial_file) as f:
                instance = pickle.load(f)
                if instance.n_priorities != job_loader.mq.n_priorities:
                    return
                instance.loader = job_loader
                instance.mq = job_loader.mq
                return instance
                    
    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop('mq', None)
        d.pop('loader', None)
        return d
    
    def save(self):
        if not os.path.exists(self.dir_):
            os.makedirs(self.dir_)
        with open(self.serial_file, 'w') as f:
            pickle.dump(self, f)
    
    def execute(self, opener):
        raise NotImplementedError
    
    def run(self):
        raise NotImplementedError
    
    def shutdown(self):
        pass

class UrlExecutor(Executor):
    pass
    
class BundleExecutor(Executor):
    def _error(self, bundle, url, e):
        self.error_times += 1
        self.loader._log_error(bundle.label, e)
        self.loader.error(bundle.label)
                    
        if url is not None: bundle.current_urls.insert(0, url)
        self.mq.put(bundle, force=True)
    
    def _exec_priority(self, opener, priority=0):
        is_inc = priority == self.n_priorities
        
        n_empty = 0
        start = time.time()
        
        time_exceed = lambda: time.time() - start > self.priorities_secs[priority]
        
        try:
            bundle = self.priorities_objs[priority].pop(0)
        except IndexError:
            bundle = None
            
        while not time_exceed() and not self.stopped:
            if bundle is None:
                if not is_inc and not self.loader.apply():
                    break
                
                if is_inc: 
                    bundle = self.mq.get_inc()
                else:
                    bundle = self.mq.get(priority=priority)
                if bundle is None:
                    n_empty += 1
                    if n_empty > EMPTY_TIMES:
                        break
                self.priorities_objs[priority].append(bundle)
            n_empty = 0
            
            if bundle is None:
                break
                
            bundle.current_urls = getattr(bundle, 'current_urls', []) or bundle.urls()
            url = None
            try:
                while len(bundle.current_urls) > 0 and not self.stopped and \
                    not time_exceed():
                    url = bundle.current_urls.pop(0)
                    self.loader.info_logger.info('get %s url: %s' % (bundle.label, url))
                    
                    parser_cls, options = self.loader.job.url_patterns.get_parser(url, options=True)
                    if parser_cls is not None:
                        self.loader._require_budget()
                        self.loader.pages_size += 1
                        
                        # !!! need to be fix, no longer return (urls, bundles), 
                        # replace with yield statement
                        next_urls, bundles = parser_cls(opener, url, bundle=bundle, 
                                                        logger=self.loader.logger, 
                                                        **options).parse()
                        next_urls = list(self.loader.job.url_patterns.matches(next_urls))
                        next_urls.extend(bundle.current_urls)
                        bundle.current_urls = next_urls
                        if bundles:
                            self.mq.put([b for b in bundles if b.force is False])
                            self.mq.put([b for b in bundles if b.force is True], force=True)
                        if hasattr(opener, 'close'):
                            opener.close()
                
                if len(bundle.current_urls) == 0:            
                    self.error_times = 0
                    
                    # put into incremental mq
                    del bundle.current_urls
                    self.mq.put_inc(bundle)
                    
                    if bundle in self.priorities_objs[priority]:
                        self.priorities_objs[priority].remove(bundle)
                    
                    self.loader.complete(bundle.label)
                else:
                    if bundle not in self.priorities_objs[priority]:
                        self.priorities_objs[priority].append(bundle)
                        
                bundle = None
                
            except LoginFailure, e:
                if not self.loader._login(opener):
                    self._error(bundle, url, e)
                elif url is not None:
                    bundle.current_urls.insert(0, url)
            except Exception, e:
                if self.loader.logger is not None and url is not None:
                    self.loader.logger.error('Error when fetch url: %s' % url)
                self._error(bundle, url, e)
                
    def execute(self, opener):
        priority = 0
        while not self.stopped:
            self._exec_priority(opener, priority=priority)
            priority = (priority+1) % (self.n_priorities+1)
            
    def run(self):
        def _call():
            opener = self.loader.job.opener_cls()
            if not self.loader._login(opener):
                return
            
            self.execute(opener)
        
        self.processes = [multiprocessing.Process(target=_call) \
                          for _ in range(self.loader.instances)]
        for process in self.processes:
            process.daemon = True
            process.start()
            
    def shutdown(self):
        self.stopped = True
        [process.join() for process in self.processes]