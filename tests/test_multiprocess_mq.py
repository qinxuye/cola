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

Created on 2014-5-15

@author: chine
'''
import unittest
import tempfile
import random
import shutil
import multiprocessing
import threading

from cola.core.unit import Url
from cola.core.rpc import ColaRPCServer
from cola.core.mq import MpMessageQueue, MpMessageQueueClient


class Test(unittest.TestCase):


    def setUp(self):
        self.port = random.randint(10000, 30000)
        self.node = 'localhost:%s' % self.port
        self.nodes = [self.node]
        self.dir_ = tempfile.mkdtemp()
        self.rpc_server = ColaRPCServer(('localhost', self.port))
        thd = threading.Thread(target=self.rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        
        self.server = MpMessageQueue(self.dir_, self.rpc_server, 
                                     self.node, self.nodes[:1], 
                                     instances=2, copies=0)

    def tearDown(self):
        try:
            self.server.shutdown()
            self.rpc_server.shutdown()
        except:
            shutil.rmtree(self.dir_)
    
    def testMpMq(self):
        
        def run1(kw):
            client1 = MpMessageQueueClient(0, kw)
            client1.put([Url('http://qinxuye.me'), '123'])
            objs = client1.get(size=2)
            self.assertEqual(objs[0].url, 'http://qinxuye.me')
            self.assertEqual(objs[1], '123')
            
        def run2(kw):
            client2 = MpMessageQueueClient(1, kw)
            client2.put(123, True)
            self.assertEqual(client2.get(), 123)
            
        m1 = multiprocessing.Process(target=run1, args=(self.server.kw, ))
        m1.start()
        m1.join()
        
        m2 = multiprocessing.Process(target=run2, args=(self.server.kw, ))
        m2.start()
        m2.join()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()