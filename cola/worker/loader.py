#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-28

@author: Chine
'''

import os
import xmlrpclib
import time
import threading

from cola.core.mq import MessageQueue
from cola.core.mq.node import Node

MAX_THREADS_SIZE = 10
TIME_SLEEP = 10

class JobLoader(object):
    def __init__(self, job, mq=None, master=None, context=None):
        self.job = job
        self.mq = mq
        self.master = master
        
        # If stop
        self.stop = False
        
        self.ctx = context or self.job.context
        
        # Thread pool
        self.instances = max(min(self.ctx.job.instances, MAX_THREADS_SIZE), 1)
        
        self.size =self.ctx.job.size
        
    def init_mq(self, rpc_server, nodes, local_node, loc, copies=1):
        mq_store_dir = os.path.join(loc, 'store')
        mq_backup_dir = os.path.join(loc, 'backup')
        if not os.path.exists(mq_store_dir):
            os.mkdir(mq_store_dir)
        if not os.path.exists(mq_backup_dir):
            os.mkdir(mq_backup_dir)
        mq_store = Node(mq_store_dir)
        mq_backup = Node(mq_backup_dir)
        
        # MQ relative
        self.mq = MessageQueue(
            nodes,
            local_node,
            rpc_server,
            mq_store,
            mq_backup,
            copies=copies
        )
        
    def stop(self):
        self.stop = True
        # sth need to do
        
        self.finish()
        
    def complete(self, obj):
        if self.ctx.job.size <= 0:
            return False
        
        if self.master is not None:
            server = xmlrpclib.ServerProxy('http://%s' % self.master)
            return server.complete(obj)
        else:
            self.size -= 1
            # sth to log
            if self.size <= 0:
                self.stop = True
            return self.stop
            
    def finish(self):
        self.mq.shutdown()
        
    def _execute(self, obj):
        # sth need to do in order to register master under limited speed
        
        if self.job.is_bundle:
            bundle = self.job.unit_cls(obj)
            urls = bundle.urls()
            
            while len(urls) > 0:
                url = urls.pop(0)
                
                parser_cls = self.job.url_patterns.get_parser(url)
                if parser_cls is not None:
                    next_urls, bundles = parser_cls(self.job.opener_cls, url).parse()
                    next_urls = list(self.job.url_patterns.matches(next_urls))
                    next_urls.extend(urls)
                    urls = next_urls
                    if bundles:
                        self.mq.put(bundles)
        else:
            parser_cls = self.job.url_patterns.get_parser(obj)
            if parser_cls is not None:
                next_urls = parser_cls(self.job.opener_cls, obj).parse()
                next_urls = list(self.job.url_patterns.matches(next_urls))
                self.mq.put(next_urls)
            
        return self.complete(obj)
        
    def run(self):
        def _call():
            stop = False
            while not self.stop and not stop:
                obj = self.mq.get()
                print 'start to get %s' % obj
                if obj is None:
                    time.sleep(TIME_SLEEP)
                    continue
                
                stop = self._execute(obj)
                
        def _callback(result):
            if not self.stop and result:
                self.pool.apply_async(_call(), callback=_callback)
                
        threads = [threading.Thread(target=_call) for _ in range(self.instances)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()