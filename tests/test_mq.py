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

Created on 2013-5-25

@author: Chine
'''
import unittest
import tempfile
import shutil
import threading
import random

from cola.core.unit import Url
from cola.core.rpc import ColaRPCServer
from cola.core.mq import MessageQueue, MessageQueueClient

class Test(unittest.TestCase):


    def setUp(self):
        ports = tuple([random.randint(10000, 30000) for _ in range(3)])
        self.nodes = ['localhost:%s'%port for port in ports]
        self.dirs = [tempfile.mkdtemp() for _ in range(len(ports))]
        self.size = len(ports)
        
        for i in range(self.size):
            setattr(self, 'rpc_server%s'%i, ColaRPCServer(('localhost', ports[i])))
            setattr(self, 'mq%s'%i, 
                MessageQueue(self.dirs[i], getattr(self, 'rpc_server%s'%i), 
                             self.nodes[i], self.nodes[:])
            )
            thd = threading.Thread(target=getattr(self, 'rpc_server%s'%i).serve_forever)
            thd.setDaemon(True)
            thd.start()
            
        self.client = MessageQueueClient(self.nodes)

    def tearDown(self):
        try:
            for i in range(self.size):
                getattr(self, 'rpc_server%s'%i).shutdown()
                getattr(self, 'mq%s'%i).shutdown()
        finally:
            for d in self.dirs:
                shutil.rmtree(d)


    def testMQ(self):
        mq = self.mq0
        data = [str(random.randint(10000, 50000)) for _ in range(20)]
                      
        mq.put(data, flush=True)
        gets = []
        while True:
            get = mq.get()
            if get is None:
                break
            gets.append(get)
                      
        self.assertEqual(sorted(data), sorted(gets))
                   
        # test mq client
        data = str(random.randint(10000, 50000))
        self.client.put(data)
        get = self.client.get()
        self.assertEqual(data, get)
             
        self.client.put(Url('http://qinxuye.me', priority=1))
        get = self.client.get(priority=1)
        self.assertEqual(get.url, 'http://qinxuye.me')
             
        # test put into different priorities
        self.client.put(Url('http://qinxuye.me', priority=0))
        self.client.put(Url('http://qinxuye.me/about', priority=1))
              
        self.assertEqual(self.client.get(priority=1).url, 'http://qinxuye.me/about')
        self.assertEqual(self.client.get(priority=0).url, 'http://qinxuye.me')
           
    def testRemoveNode(self):
        mq = self.mq0
        data = [str(i) for i in range(100)]
                  
        # test remove node
        mq.put(data)
        self.mq2.shutdown()
        self.mq0.remove_node(self.nodes[2])
        self.mq1.remove_node(self.nodes[2])
                  
        gets = []
        while True:
            get = mq.get()
            if get is None:
                break
            gets.append(get)
               
        self.assertEqual(sorted(data), sorted(gets))
         
    def testAddNode(self):
        data = range(100)
          
        new_port = random.randint(10000, 30000)
        new_node = 'localhost:%s' % new_port
        new_rpc_server = ColaRPCServer(('localhost', new_port))
        thd = threading.Thread(target=new_rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        new_dir = tempfile.mkdtemp()
        ns = list(self.nodes)
        ns.append(new_node)
        new_mq = MessageQueue(new_dir, new_rpc_server, new_node, ns)
          
        try:
            self.mq0.add_node(new_node)
            self.mq1.add_node(new_node)
            self.mq2.add_node(new_node)
              
            self.mq0.put(data)
              
            self.assertEqual(data, sorted(self.mq0.get(size=100)))
        finally:
            try:
                new_rpc_server.shutdown()
                new_mq.shutdown()
            finally:
                shutil.rmtree(new_dir)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()