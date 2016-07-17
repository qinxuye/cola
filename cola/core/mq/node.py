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

Created on 2014-4-27

@author: chine
'''

import os
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import defaultdict
import socket

from cola.core.rpc import client_call
from cola.core.utils import get_rpc_prefix
from cola.core.mq.store import Store
from cola.core.mq.distributor import Distributor
    
MQ_STATUS_FILENAME = 'mq.status' # file name of message queue status

PRIORITY_STORE_FN = 'store'
BACKUP_STORE_FN = 'backup'
INCR_STORE_FN = 'inc'

CACHE_SIZE = 20


class LocalMessageQueueNode(object):
    """
    Message queue node which only handle local mq operations
    can also be in charge of handling the remote call.
    This node includes several storage such as priority storage
    for each priority, incremental storage as well as the
    backup storage. Each storage is an instance of
    :class:`~cola.core.mq.store.Store`.
    """
    def __init__(self, base_dir, rpc_server, addr, addrs,
                 copies=1, n_priorities=3, deduper=None,
                 app_name=None):
        self.dir_ = base_dir
        self.rpc_server = rpc_server
        
        assert addr in addrs
        self.addr = addr
        self.addrs = addrs
        self.other_addrs = [n for n in self.addrs if n != self.addr]
        
        self.copies = max(min(len(self.addrs)-1, copies), 0)
        self.n_priorities = max(n_priorities, 1)
        self.deduper = deduper
        self.app_name = app_name
        
        self._lock = threading.Lock()
        
        self._register_rpc()
        
        self.inited = False
        
    def init(self):
        with self._lock:
            if self.inited: return
            
            get_priority_store_dir = lambda priority: os.path.join(self.dir_, 
                                        PRIORITY_STORE_FN, str(priority))
            self.priority_stores = [Store(get_priority_store_dir(i), 
                                          deduper=self.deduper,
                                          mkdirs=True) \
                                    for i in range(self.n_priorities)]
            
    
            backup_store_dir = os.path.join(self.dir_, BACKUP_STORE_FN)
            self.backup_stores = {}
            for backup_addr in self.other_addrs:
                backup_node_dir = backup_addr.replace(':', '_')
                backup_path = os.path.join(backup_store_dir, backup_node_dir)
                self.backup_stores[backup_addr] = Store(backup_path, 
                                                       size=512*1024, mkdirs=True)
                
            inc_store_dir = os.path.join(self.dir_, INCR_STORE_FN)
            self.inc_store = Store(inc_store_dir, mkdirs=True)
                    
            self.inited = True
        
    def _register_rpc(self):
        if self.rpc_server:
            self.register_rpc(self, self.rpc_server, app_name=self.app_name)
                
    @classmethod
    def register_rpc(cls, node, rpc_server, app_name=None):
        prefix = get_rpc_prefix(app_name, 'mq')
        rpc_server.register_function(node.put_proxy, name='put', 
                                     prefix=prefix)
        rpc_server.register_function(node.batch_put_proxy, name='batch_put', 
                                     prefix=prefix)
        rpc_server.register_function(node.put_backup_proxy, name='put_backup',
                                     prefix=prefix)
        rpc_server.register_function(node.get_proxy, name='get',
                                     prefix=prefix)
        rpc_server.register_function(node.exist, name='exist',
                                     prefix=prefix)

    def put(self, objs, force=False, priority=0):
        self.init()

        priority = max(min(priority, self.n_priorities-1), 0)
        priority_store = self.priority_stores[priority]
        priority_store.put(objs, force=force)
        
    def put_proxy(self, pickled_objs, force=False, priority=0):
        """
        The objects from remote call should be pickled to
        avoid the serialization error.

        :param pickled_objs: the pickled objects to put into mq
        :param force: if set to True will be directly put into mq without
               checking the duplication
        :param priority: the priority queue to put into
        """
        objs = pickle.loads(pickled_objs)
        self.put(objs, force=force, priority=priority)
        
    def batch_put(self, objs):
        self.init()
        
        puts = defaultdict(lambda:defaultdict(list))
        for obj in objs:
            priority = getattr(obj, 'priority', 0)
            force = getattr(obj, 'force', False)
            puts[priority][force].append(obj)
        
        for priority, m in puts.iteritems():
            for force, obs in m.iteritems():
                self.put(obs, force=force, priority=priority)
                
    def batch_put_proxy(self, pickled_objs):
        """
        Unlike the :func:`put`, this method will check the ``priority``
        of a single object to decide which priority queue to put into.
        """
        objs = pickle.loads(pickled_objs)
        self.batch_put(objs)
    
    def put_backup(self, addr, objs, force=False):
        self.init()
        
        backup_store = self.backup_stores[addr]
        backup_store.put(objs, force=force)
        
    def put_backup_proxy(self, addr, pickled_objs, force=False):
        """
        In the Cola backup mechanism, an object will not only be
        put into a hash ring node, and also be put into the next
        hash ring node which marked as a backup node. To the backup node,
        it will remember the previous node's name.

        :param addr: the node address to backup
        :param pickled_objs: pickled objects
        :param force: if True will be put into queue without checking duplication
        """
        objs = pickle.loads(pickled_objs)
        self.put_backup(addr, objs, force=force)
        
    def put_inc(self, objs, force=True):
        self.init()
        
        self.inc_store.put(objs, force=force)
        
    def get(self, size=1, priority=0):
        self.init()
        
        priority = max(min(priority, self.n_priorities-1), 0)
        priority_store = self.priority_stores[priority]
        return priority_store.get(size=size)
    
    def get_proxy(self, size=1, priority=0):
        """
        Get the objects from the specific priority queue.

        :param size: if size == 1 will be the right object,
               else will be the objects list
        :param priority:
        :return: unpickled objects
        """
        return pickle.dumps(self.get(size=size, priority=priority))
    
    def get_backup(self, addr, size=1):
        self.init()
        
        backup_store = self.backup_stores[addr]
        return backup_store.get(size=size)
    
    def get_inc(self, size=1):
        self.init()
        
        return self.inc_store.get(size=size)
    
    def add_node(self, addr):
        """
        When a new message queue node is in, firstly will add the address
        to the known queue nodes, then a backup for this node will be created.
        """
        if addr in self.addrs: return
        
        self.addrs.append(addr)
        
        backup_store_dir = os.path.join(self.dir_, BACKUP_STORE_FN)
        backup_node_dir = addr.replace(':', '_')
        backup_path = os.path.join(backup_store_dir, backup_node_dir)
        self.backup_stores[addr] = Store(backup_path, 
                                         size=512*1024, mkdirs=True)
        
    def remove_node(self, addr):
        """
        For the removed node, this method is for the cleaning job including
        shutting down the backup storage for the removed node.
        """
        if addr not in self.addrs: return
        
        self.addrs.remove(addr)
        self.backup_stores[addr].shutdown()
        del self.backup_stores[addr]
        
    def exist(self, obj):
        if self.deduper:
            return self.deduper.exist(str(obj))
        return False
    
    def shutdown(self):
        if not self.inited: return
        
        [store.shutdown() for store in self.priority_stores]
        for backup_store in self.backup_stores.values():
            backup_store.shutdown()
        self.inc_store.shutdown()


class MessageQueueNodeProxy(object):
    """
    This class maintains an instance of :class:`~cola.core.mq.node.LocalMessageQueueNode`,
    and provide `PUT` and `GET` relative method.
    In each mq operation, it will execute a local or remote call by judging the address.
    The Remote call will actually send a RPC to the destination worker's instance which
    execute the method provided by :class:`~cola.core.mq.node.LocalMessageQueueNode`.

    Besides, this class also maintains an instance of :class:`~cola.core.mq.distributor.Distributor`
    which holds a hash ring. To an object of `PUT` operation, the object should be distributed to
    the destination according to the mechanism of the hash ring. Remember, a cache will be created
    to avoid the frequent write operations which may cause high burden of a message queue node.
    To `GET` operation, the mq will just fetch an object from the local node,
    or request from other nodes if local one's objects are exhausted.
    """
    def __init__(self, base_dir, rpc_server, addr, addrs,
                 copies=1, n_priorities=3, deduper=None,
                 app_name=None, logger=None):
        self.dir_ = base_dir
        self.addr_ = addr
        self.addrs = list(addrs)
        self.mq_node = LocalMessageQueueNode(
            base_dir, rpc_server, addr, addrs, 
            copies=copies, n_priorities=n_priorities, deduper=deduper,
            app_name=app_name)
        self.distributor = Distributor(addrs, copies=copies)
        self.logger = logger
        
        self.prefix = get_rpc_prefix(app_name, 'mq')
        
        self._lock = threading.Lock()
        self.inited = False
        
    @classmethod
    def register_rpc(cls, node, rpc_server, app_name=None):
        LocalMessageQueueNode.register_rpc(node.mq_node, rpc_server, 
                                           app_name=app_name)
        
    def init(self):
        with self._lock:
            if self.inited: return
            
            self.load()
            if not hasattr(self, 'caches'):
                self.caches = dict((addr, []) for addr in self.addrs)
            if not hasattr(self, 'caches_inited'):
                self.caches_inited = dict((addr, False) for addr in self.addrs)
            if not hasattr(self, 'backup_caches'):
                self.backup_caches = dict((addr, {}) for addr in self.addrs)
                for addr in self.addrs:
                    for other_addr in [n for n in self.addrs if addr != n]:
                        self.backup_caches[addr][other_addr] = []
                
            self.mq_node.init()
            self.inited = True
        
    def load(self):
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        if not os.path.exists(save_file):
            return
        
        with open(save_file, 'r') as f:
            self.caches, self.caches_inited, self.backup_caches = pickle.load(f)
    
    def save(self):
        if not self.inited:
            return
        
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            t = (self.caches, self.caches_inited, self.backup_caches)
            pickle.dump(t, f)
        
    def _check_empty(self, objs):
        if objs is None:
            return True
        elif isinstance(objs, list) and len(objs) == 0:
            return True
        return False
            
    def _remote_or_local_put(self, addr, objs, force=False, priority=0):
        if self._check_empty(objs):
            return
        if addr == self.addr_:
            self.mq_node.put(objs, force=force, priority=priority)
        else:
            client_call(addr, self.prefix+'put', pickle.dumps(objs), 
                        force, priority)
            
    def _remote_or_local_batch_put(self, addr, objs):
        if self._check_empty(objs):
            return
        if addr == self.addr_:
            self.mq_node.batch_put(objs)
        else:
            client_call(addr, self.prefix+'batch_put', pickle.dumps(objs))
            
    def _remote_or_local_get(self, addr, size=1, priority=0):
        objs = None
        if addr == self.addr_:
            objs = self.mq_node.get(size=size, priority=priority)
        else:
            objs = pickle.loads(client_call(addr, self.prefix+'get', 
                                            size, priority))
        
        addr_caches = self.caches.get(addr, [])
        if size == 1 and objs is None and len(addr_caches) > 0:
            return addr_caches.pop(0)
        elif size > 1 and len(objs) == 0 and len(addr_caches) > 0:
            return addr_caches[:size]
        
        return objs
            
    def _remote_or_local_put_backup(self, addr, backup_addr, objs, 
                                    force=False):
        if self._check_empty(objs):
            return
        if addr == self.addr_:
            self.mq_node.put_backup(backup_addr, objs, force=force)
        else:
            client_call(addr, self.prefix+'put_backup', backup_addr, 
                        pickle.dumps(objs), force)
                    
    def put(self, objects, flush=False):
        """
        Put a bunch of objects into the mq. The objects will be distributed
        to different mq nodes according to the instance of
        :class:`~cola.core.mq.distributor.Distributor`.
        There also exists a cache which will not flush out unless the parameter flush
        is true or a single destination cache is full.

        :param objects: objects to put into mq, an object is mostly the instance of
               :class:`~cola.core.unit.Url` or :class:`~cola.core.unit.Bundle`
        :param flush: flush out the cache all if set to true
        """
        self.init()
        
        addrs_objs, backup_addrs_objs = \
            self.distributor.distribute(objects)
            
        if flush is True:
            for addr in self.addrs:
                if addr not in addrs_objs:
                    addrs_objs[addr] = []
                if addr not in backup_addrs_objs:
                    backup_addrs_objs[addr] = {}
            
        for addr, objs in addrs_objs.iteritems():
            self.caches[addr].extend(objs)
            if not self.caches_inited[addr] or \
                len(self.caches[addr]) >= CACHE_SIZE or flush:
                try:
                    self._remote_or_local_batch_put(addr, self.caches[addr])
                except socket.error, e:
                    if self.logger:
                        self.logger.exception(e)
                else:
                    self.caches[addr] = []
                
            if not self.caches_inited[addr]:
                self.caches_inited[addr] = True
        
        for addr, m in backup_addrs_objs.iteritems():
            for backup_addr, objs in m.iteritems():
                self.backup_caches[addr][backup_addr].extend(objs)
            
            size = sum([len(obs) for obs in \
                            self.backup_caches[addr].values()])
            if size >= CACHE_SIZE or flush:
                for backup_addr, objs in self.backup_caches[addr].iteritems():
                    try:
                        self._remote_or_local_put_backup(
                            addr, backup_addr, objs)
                    except socket.error, e:
                        if self.logger:
                            self.logger.exception(e)
                    else:
                        self.backup_caches[addr][backup_addr] = []
            
    def get(self, size=1, priority=0):
        """
        Get a bunch of objects from the message queue.
        This method will try to fetch objects from local node as much as wish.
        If not enough, will try to fetch from the other nodes.

        :param size: the objects wish to fetch
        :param priority: the priority queue which wants to fetch from
        :return: the objects which to handle
        """
        self.init()
        
        if size < 1: size = 1
        results = []
        _addrs = sorted(self.addrs, key=lambda k: k==self.addr_, 
                             reverse=True)
        
        for addr in _addrs:
            left = size - len(results)
            if left <= 0:
                break
            
            objs = None
            try:
                objs = self._remote_or_local_get(addr, size=left, 
                                                 priority=priority)
            except socket.error, e:
                if self.logger:
                    self.logger.exception(e)
                    
            if objs is None:
                continue
            if not isinstance(objs, list):
                objs = [objs, ]
            results.extend(objs)
        
        if size == 1:
            if len(results) == 0:
                return
            return results[0]
        return results
    
    def put_inc(self, objs):
        self.mq_node.put_inc(objs)
        
    def get_inc(self, size=1):
        return self.mq_node.get_inc(size=size)
    
    def flush(self):
        self.put([], flush=True)
    
    def add_node(self, addr):
        if addr in self.addrs: return
        
        self.init()
        
        self.distributor.add_node(addr)
        self.addrs.append(addr)
                
        self.caches[addr] = []
        self.caches_inited[addr] = False
        self.backup_caches[addr] = {}
        for o_addr in self.addrs:
            if o_addr != addr:
                self.backup_caches[addr][o_addr] = []
                self.backup_caches[o_addr][addr] = []
                
        self.mq_node.add_node(addr)
    
    def remove_node(self, addr):
        if addr not in self.addrs: return
        
        self.init()
        
        self.distributor.remove_node(addr)
        self.addrs.remove(addr)
                
        self.mq_node.batch_put(self.caches[addr])
        del self.caches[addr]
        del self.caches_inited[addr]
        del self.backup_caches[addr]
        for o_addr in self.addrs:
            if o_addr != addr:
                del self.backup_caches[o_addr][addr]
         
        self.flush()
        
        BATCH_SIZE = 10
        objs = self.mq_node.get_backup(addr, size=BATCH_SIZE)
        while len(objs) > 0:
            self.mq_node.batch_put(objs)
            objs = self.mq_node.get_backup(addr, size=BATCH_SIZE)
        
        self.mq_node.remove_node(addr)
        
    def exist(self, obj):
        return self.mq_node.exist(obj)
    
    def shutdown(self):
        if not self.inited: return
        
        self.mq_node.shutdown()
        self.save()
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.shutdown()