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
import socket
import logging

from cola.core.mq import MessageQueue
from cola.core.bloomfilter import FileBloomFilter
from cola.core.rpc import client_call
from cola.core.utils import get_ip, root_dir
from cola.core.errors import ConfigurationError
from cola.core.logs import get_logger
from cola.core.utils import import_job
from cola.core.errors import LoginFailure
from cola.job.loader import JobLoader, LimitionJobLoader

MAX_THREADS_SIZE = 10
TIME_SLEEP = 10
BUDGET_REQUIRE = 10
MAX_ERROR_TIMES = 5

UNLIMIT_BLOOM_FILTER_CAPACITY = 1000000

class JobWorkerRunning(Exception): pass

class BasicWorkerJobLoader(JobLoader):
    def __init__(self, job, data_dir, context=None, logger=None,
                 local=None, nodes=None, copies=1, force=False):
        self.job = job
        ctx = context or self.job.context
        
        self.local = local
        if self.local is None:
            host, port = get_ip(), ctx.job.port
            self.local = '%s:%s' % (host, port)
        else:
            host, port = tuple(self.local.split(':', 1))
        self.nodes = nodes
        if self.nodes is None:
            self.nodes = [self.local]
            
        self.logger = logger
        self.info_logger = get_logger(
            name='cola_worker_info_%s'%self.job.real_name)
            
        super(BasicWorkerJobLoader, self).__init__(
            self.job, data_dir, self.local, 
            context=ctx, copies=copies, force=force)
        
        # instances count that run at the same time
        self.instances = max(min(self.ctx.job.instances, MAX_THREADS_SIZE), 1)
        # excecutings
        self.executings = []
        # exception times that continously throw
        self.error_times = 0
        # budget
        self.budget = 0
        
        self.check()
        # init rpc server
        self.init_rpc_server()
        # init message queue
        self.init_mq()
        
        # register signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.rpc_server.register_function(self.stop, name='stop')
        self.rpc_server.register_function(self.add_node, name='add_node')
        self.rpc_server.register_function(self.remove_node, name='remove_node')
        self.rpc_server.register_function(self.run, name='run')
            
    def _init_bloom_filter(self):
        size = self.job.context.job.size
        base = 1 if not self.job.is_bundle else 1000 
        bloom_filter_file = os.path.join(self.root, 'bloomfilter')
        
        if not os.path.exists(bloom_filter_file):
            if size > 0:
                bloom_filter_size = size*10*base
            else:
                bloom_filter_size = UNLIMIT_BLOOM_FILTER_CAPACITY
        else:
            if size > 0:
                bloom_filter_size = size*2*base
            else:
                bloom_filter_size = UNLIMIT_BLOOM_FILTER_CAPACITY
        return FileBloomFilter(bloom_filter_file, bloom_filter_size)
            
    def init_mq(self):
        mq_store_dir = os.path.join(self.root, 'store')
        mq_backup_dir = os.path.join(self.root, 'backup')
        if not os.path.exists(mq_store_dir):
            os.makedirs(mq_store_dir)
        if not os.path.exists(mq_backup_dir):
            os.makedirs(mq_backup_dir)
            
        self.mq = MessageQueue(self.nodes, self.local, self.rpc_server,
            copies=self.copies)
        self.mq.init_store(mq_store_dir, mq_backup_dir, 
                           verify_exists_hook=self._init_bloom_filter())
        
    def check(self):
        env_legal = self.check_env(force=self.force)
        if not env_legal:
            raise JobWorkerRunning('There has been a running job worker.')
        
    def finish(self):
        self.stopped = True
        self.mq.shutdown()
        try:
            for handler in self.logger.handlers:
                handler.close()
        finally:
            super(BasicWorkerJobLoader, self).finish()
        
    def complete(self, obj):
        if self.logger is not None:
            self.logger.info('Finish %s' % obj)
        if obj in self.executings:
            self.executings.remove(obj)
        
        if self.ctx.job.size <= 0:
            return True
        return False
            
    def error(self, obj):
        if obj in self.executings:
            self.executings.remove(obj)
        
    def stop(self):
        self.mq.put(self.executings, force=True)
        super(BasicWorkerJobLoader, self).stop()
        
    def signal_handler(self, signum, frame):
        self.stop()
        
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
        return True
        
    def _log_error(self, obj, err):
        if self.logger is not None:
            self.logger.error('Error when get bundle: %s' % obj)
            self.logger.exception(err)
            
        if self.job.debug:
            raise err
        
    def _require_budget(self, count):
        raise NotImplementedError
    
    def apply(self):
        raise NotImplementedError
    
    def _execute_bundle(self, obj, opener=None):
        bundle = self.job.unit_cls(obj)
        urls = bundle.urls()
        
        url = None
        try:
            while len(urls) > 0 and not self.stopped:
                url = urls.pop(0)
                self.info_logger.info('get %s url: %s' % (bundle.label, url))
                
                parser_cls, options = self.job.url_patterns.get_parser(url, options=True)
                if parser_cls is not None:
                    self._require_budget()
                    next_urls, bundles = parser_cls(opener, url, bundle=bundle, logger=self.logger, 
                                                    **options).parse()
                    next_urls = list(self.job.url_patterns.matches(next_urls))
                    next_urls.extend(urls)
                    urls = next_urls
                    if bundles:
                        self.mq.put([str(b) for b in bundles if b.force is False])
                        self.mq.put([str(b) for b in bundles if b.force is True], force=True)
                    if hasattr(opener, 'close'):
                        opener.close()
                        
            self.error_times = 0
        except LoginFailure, e:
            if not self._login(opener):
                self.error_times += 1
                self._log_error(obj, e)
                self.error(obj)
        except Exception, e:
            self.error_times += 1
            if self.logger is not None and url is not None:
                self.logger.error('Error when fetch url: %s' % url)
            self._log_error(obj, e)
            self.error(obj)
            
    def _execute_url(self, obj, opener=None):
        self._require_budget()
        try:
            parser_cls, options = self.job.url_patterns.get_parser(obj, options=True)
            if parser_cls is not None:
                next_urls = parser_cls(opener, obj, logger=self.logger, **options).parse()
                next_urls = list(self.job.url_patterns.matches(next_urls))
                
                puts = []
                forces = []
                for url in next_urls:
                    if isinstance(url, basestring) or url.force is False:
                        puts.append(url)
                    else:
                        forces.append(url)
                self.mq.put(puts)
                self.mq.put(forces, force=True)
                if hasattr(opener, 'close'):
                    opener.close()
                
            self.error_times = 0
        except LoginFailure, e:
            if not self._login(opener):
                self.error_times += 1
                self._log_error(obj, e)
                self.error(obj)
        except Exception, e:
            self.error_times += 1
            self._log_error(obj, e)
            self.error(obj)
            
    def execute(self, obj, opener=None):
        '''
        return True means all finished
        '''
        # If reaches continous erros maxium
        if self.error_times >= MAX_ERROR_TIMES:
            return True
        
        if opener is None:
            opener = self.job.opener_cls()
            
        if self.job.is_bundle:
            self._execute_bundle(obj, opener=opener)
        else:
            self._execute_url(obj, opener=opener)
            
        return self.complete(obj)
        
    def remove_node(self, node):
        if self.mq is not None:
            self.mq.remove_node(node)
            
    def add_node(self, node):
        if self.mq is not None:
            self.mq.add_node(node)
            
    def _run(self):
        def _call(opener=None):
            if opener is None:
                opener = self.job.opener_cls()
            if not self._login(opener):
                return
            
            stopped = False
            while not self.stopped and not stopped:
                obj = self.mq.get()
                self.info_logger.info('start to get %s' % obj)
                if obj is None:
                    time.sleep(TIME_SLEEP)
                    continue
                
                if not self.apply():
                    return True
                
                self.executings.append(obj)
                stopped = self.execute(obj, opener=opener)
                
        try:
            threads = [threading.Thread(target=_call) for _ in range(self.instances)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            self.finish()
            
    def run(self):
        raise NotImplementedError
    
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.finish()
        
class StandaloneWorkerJobLoader(LimitionJobLoader, BasicWorkerJobLoader):
    def __init__(self, job, data_dir, master=None, local=None, nodes=None, 
                 context=None, logger=None, copies=1, force=False):
        BasicWorkerJobLoader.__init__(self, job, data_dir, context=context, logger=logger,
                                      local=local, nodes=nodes, copies=copies, force=force)
        LimitionJobLoader.__init__(self, self.job, context=context)
        
        log_level = logging.INFO if not job.debug else logging.DEBUG
        if self.logger is None:
            self.logger = get_logger(
                name='cola_worker_%s'%self.job.real_name,
                filename=os.path.join(self.root, 'job.log'),
                basic_level=log_level)
            
        self.init_rate_clear()
        
    def finish(self):
        LimitionJobLoader.finish(self)
        BasicWorkerJobLoader.finish(self)
        
    def stop(self):
        LimitionJobLoader.stop(self)
        BasicWorkerJobLoader.stop(self)
        
    def complete(self, obj):
        BasicWorkerJobLoader.complete(self, obj)
        return LimitionJobLoader.complete(self, obj)
    
    def error(self, obj):
        LimitionJobLoader.error(self, obj)
        BasicWorkerJobLoader.error(self, obj)
            
    def _require_budget(self):
        if not self.rate_limit or self.stopped:
            return
        
        if self.budget > 0:
            self.budget -= 1
            return
        
        while self.budget == 0 and not self.stopped:
            self.budget = self.require(BUDGET_REQUIRE)
            if self.budget > 0:
                self.budget -= 1
                return
            
    def run(self, put_starts=True):
        if put_starts:
            self.mq.put(self.job.starts)
        self._run()
        
class WorkerJobLoader(BasicWorkerJobLoader):
    def __init__(self, job, data_dir, master, local=None, nodes=None, 
                 context=None, logger=None, copies=1, force=False):
        super(WorkerJobLoader, self).__init__(job, data_dir, context=context, logger=logger, 
                                              local=local, nodes=nodes, copies=copies, force=force)
        log_level = logging.INFO if not job.debug else logging.DEBUG
        if self.logger is None:
            self.logger = get_logger(
                name='cola_worker_%s'%self.job.real_name,
                filename=os.path.join(self.root, 'job.log'),
                server=master.split(':')[0],
                basic_level=log_level)
            
        self.master = master
        self.run_lock = threading.Lock()
        self.run_lock.acquire()
        
    def apply(self):
        return client_call(self.master, 'apply')
            
    def complete(self, obj):
        super(WorkerJobLoader, self).complete(obj)
        return client_call(self.master, 'complete', obj)
    
    def error(self, obj):
        super(WorkerJobLoader, self).error(obj)
        client_call(self.master, 'error', obj)
        
    def _require_budget(self):
        if self.ctx.job.limits == 0 or self.stopped:
            return
        
        if self.budget > 0:
            self.budget -= 1
            return
        
        while self.budget == 0 and not self.stopped:
            self.budget = client_call(self.master, 'require', BUDGET_REQUIRE)
            if self.budget > 0:
                self.budget -= 1
                return
        
    def ready_for_run(self):
        self.run_lock.acquire()
        self._run()
        
    def run(self):
        self.run_lock.release()
        
    def finish(self):
        super(WorkerJobLoader, self).finish()
        try:
            client_call(self.master, 'worker_finish', self.local)
        except socket.error:
            pass

def load_job(job_path, data_path=None, master=None, force=False):
    if not os.path.exists(job_path):
        raise ValueError('Job definition does not exist.')
        
    job = import_job(job_path)
    
    if data_path is None:
        data_path = os.path.join(root_dir(), 'data')
    root = os.path.join(
        data_path, 'worker', 'jobs', job.real_name)
    if not os.path.exists(root):
        os.makedirs(root)
    
    if master is None:
        with StandaloneWorkerJobLoader(job, root, force=force) as job_loader:
            job_loader.run()
    else:
        nodes = client_call(master, 'get_nodes')
        local = '%s:%s' % (get_ip(), job.context.job.port)
        client_call(master, 'ready', local)
        with WorkerJobLoader(job, root, master, local=local, nodes=nodes, force=force) \
            as job_loader:
            client_call(master, 'ready', local)
            job_loader.ready_for_run()
            
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser('Cola job loader')
    parser.add_argument('-j', '--job', metavar='job directory', required=True,
                        help='job directory to run')
    parser.add_argument('-d', '--data', metavar='data root directory', nargs='?',
                        default=None, const=None, 
                        help='root directory to put data')
    parser.add_argument('-m', '--master', metavar='master job loader', nargs='?',
                        default=None, const=None,
                        help='master connected to(in the former of `ip:port`)')
    parser.add_argument('-f', '--force', metavar='force start', nargs='?',
                        default=False, const=True, type=bool)
    args = parser.parse_args()
    
    path = args.job
    data_path = args.data
    master = args.master
    force = args.force
    load_job(path, data_path=data_path, master=master, force=force)