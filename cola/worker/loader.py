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

Created on 2013-5-28

@author: Chine
'''

import os
import time
import threading
import signal
import random
import sys

from cola.core.mq import MessageQueue
from cola.core.bloomfilter import FileBloomFilter
from cola.core.rpc import ColaRPCServer, client_call
from cola.core.utils import get_ip, root_dir
from cola.core.errors import ConfigurationError
from cola.core.logs import get_logger
from cola.core.utils import import_job
from cola.core.errors import LoginFailure

MAX_THREADS_SIZE = 10
TIME_SLEEP = 10
BUDGET_REQUIRE = 10

UNLIMIT_BLOOM_FILTER_CAPACITY = 100000

class JobLoader(object):
    def __init__(self, job, rpc_server, 
                 mq=None, logger=None, master=None, context=None):
        self.job = job
        self.rpc_server = rpc_server
        self.mq = mq
        self.master = master
        self.logger = logger
        
        # If stop
        self.stopped = False
        
        self.ctx = context or self.job.context
        self.instances = max(min(self.ctx.job.instances, MAX_THREADS_SIZE), 1)
        self.size =self.ctx.job.size
        self.budget = 0
        
        # The execute unit
        self.executing = None
        
        # register signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        rpc_server.register_function(self.stop, name='stop')
        rpc_server.register_function(self.add_node, name='add_node')
        rpc_server.register_function(self.remove_node, name='remove_node')
        rpc_server.register_function(self.run, name='run')
        
    def init_mq(self, nodes, local_node, loc, 
                verify_exists_hook=None, copies=1):
        mq_store_dir = os.path.join(loc, 'store')
        mq_backup_dir = os.path.join(loc, 'backup')
        if not os.path.exists(mq_store_dir):
            os.mkdir(mq_store_dir)
        if not os.path.exists(mq_backup_dir):
            os.mkdir(mq_backup_dir)
        
        # MQ relative
        self.mq = MessageQueue(
            nodes,
            local_node,
            self.rpc_server,
            copies=copies
        )
        self.mq.init_store(mq_store_dir, mq_backup_dir, 
                           verify_exists_hook=verify_exists_hook)
        
    def stop(self):
        self.stopped = True
        
        if self.executing is not None:
            self.mq.put(self.executing)
        
        self.finish()
        
    def signal_handler(self, signum, frame):
        self.stop()
        
    def complete(self, obj):
        if self.logger is not None:
            self.logger.info('Finish %s' % obj)
        
        if self.ctx.job.size <= 0:
            return False
        
        self.executing = None
        if self.master is not None:
            return client_call(self.master, 'complete', obj)
        else:
            self.size -= 1
            # sth to log
            if self.size <= 0:
                self.stopped = True
            return self.stopped
            
    def finish(self):
        self.mq.shutdown()
        self.stopped = True
        
    def _require_budget(self):
        if self.master is None or self.ctx.job.limits == 0:
            return
        
        if self.budget > 0:
            self.budget -= 1
            return
        
        while self.budget == 0 and not self.stopped:
            self.budget = client_call(self.master, 'require', BUDGET_REQUIRE)
            
    def _log(self, obj, err):
        if self.logger is not None:
            self.logger.error('Error when get bundle: %s' % obj)
            self.logger.exception(err)
            
        if self.job.debug:
            raise err
        
    def _login(self, opener):
        if self.job.login_hook is not None:
            if 'login' not in self.ctx.job or \
                not isinstance(self.ctx.job.login, list):
                raise ConfigurationError('If login_hook set, config files must contains `login`')
            kw = random.choice(self.ctx.job.login)
            login_success = self.job.login_hook(opener, **kw)
            if not login_success:
                self.logger.info('login fail')
            return login_success
        
    def _execute(self, obj, opener=None):
        if opener is None:
            opener = self.job.opener_cls()
            
        if self.job.is_bundle:
            bundle = self.job.unit_cls(obj)
            urls = bundle.urls()
            
            try:
                
                while len(urls) > 0 and not self.stopped:
                    url = urls.pop(0)
                    self.logger.info('get %s url: %s' % (bundle.label, url))
                    
                    parser_cls = self.job.url_patterns.get_parser(url)
                    if parser_cls is not None:
                        self._require_budget()
                        next_urls, bundles = parser_cls(opener, url, bundle=bundle).parse()
                        next_urls = list(self.job.url_patterns.matches(next_urls))
                        next_urls.extend(urls)
                        urls = next_urls
                        if bundles:
                            self.mq.put([str(b) for b in bundles])
            except LoginFailure:
                if not self._login(opener):
                    return
            except Exception, e:
                self._log(obj, e)
                
        else:
            self._require_budget()
            
            try:
                
                parser_cls = self.job.url_patterns.get_parser(obj)
                if parser_cls is not None:
                    next_urls = parser_cls(opener, obj).parse()
                    next_urls = list(self.job.url_patterns.matches(next_urls))
                    self.mq.put(next_urls)
            
            except LoginFailure:
                if not self._login(opener):
                    return
            except Exception, e:
                self._log(obj, e)
                    
            
        return self.complete(obj)
        
    def run(self):
        def _call():
            opener = self.job.opener_cls()
            if not self._login(opener):
                return
            
            stopped = False
            while not self.stopped and not stopped:
                obj = self.mq.get()
                print 'start to get %s' % obj
                if obj is None:
                    time.sleep(TIME_SLEEP)
                    continue
                
                self.executing = obj
                stopped = self._execute(obj, opener=opener)
                
        try:
            threads = [threading.Thread(target=_call) for _ in range(self.instances)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            self.finish()
            
    def remove_node(self, node):
        if self.mq is not None:
            self.mq.remove_node(node)
            
    def add_node(self, node):
        if self.mq is not None:
            self.mq.add_node(node)

def create_rpc_server(job, context=None):
    ctx = context or job.context
    rpc_server = ColaRPCServer((get_ip(), ctx.job.port))
    thd = threading.Thread(target=rpc_server.serve_forever)
    thd.setDaemon(True)
    thd.start()
    return rpc_server

def create_bloom_filter_hook(bloom_filter_file, job):
    size = job.context.job.size
    if not os.path.exists(bloom_filter_file):
        bloom_filter_size = size*10
    else:
        if size > 0:
            bloom_filter_size = size*2
        else:
            bloom_filter_size = UNLIMIT_BLOOM_FILTER_CAPACITY
    return FileBloomFilter(bloom_filter_file, bloom_filter_size)

def load_job(path, master=None):
    if not os.path.exists(path):
        raise ValueError('Job definition does not exist.')
        
    job = import_job(path)
    
    holder = os.path.join(
        root_dir(), 'data', 'worker', 'jobs', job.real_name)
    mq_holder = os.path.join(holder, 'mq')
    if not os.path.exists(mq_holder):
        os.makedirs(mq_holder)
    
    # Logger
    logger = get_logger(os.path.join(holder, 'job.log'))
    
    local_node = '%s:%s' % (get_ip(), job.context.job.port)
    nodes = [local_node]
    if master is not None:
        nodes = client_call(master, 'get_nodes')
    
    # Bloom filter hook
    bloom_filter_file = os.path.join(holder, 'bloomfilter')
    bloom_filter_hook = create_bloom_filter_hook(bloom_filter_file, job)
    
    rpc_server = create_rpc_server(job)
    loader = JobLoader(job, rpc_server, logger=logger, master=master)
    loader.init_mq(nodes, local_node, mq_holder, 
                   verify_exists_hook=bloom_filter_hook,
                   copies=2 if master else 1)
    
    if master is None:
        try:
            loader.mq.put(job.starts)
            loader.run()
        finally:
            rpc_server.shutdown()
    else:
        try:
            client_call(master, 'ready', local_node)
            
            def _start():
                while not loader.stopped: 
                    time.sleep(TIME_SLEEP)
                loader.run()
            thread = threading.Thread(target=_start)
            thread.start()
            thread.join()
        finally:
            rpc_server.shutdown()
            
if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError('Worker job loader need at least 1 parameters.')
    
    path = sys.argv[1]
    master = None
    if len(sys.argv) > 2:
        master = sys.argv[2]
    load_job(path, master=master)