'''
Created on 2013-5-25

@author: Chine
'''
import unittest
import tempfile
import shutil
import threading
import random

from cola.core.rpc import ColaRPCServer
from cola.core.mq import MessageQueue
from cola.core.mq.client import MessageQueueClient

class Test(unittest.TestCase):


    def setUp(self):
        ports = (11111, 11211, 11311)
        self.nodes = ['localhost:%s'%port for port in ports]
        self.dirs = [tempfile.mkdtemp() for _ in range(2*len(ports))]
        self.size = len(ports)
        
        for i in range(self.size):
            setattr(self, 'rpc_server%s'%i, ColaRPCServer(('localhost', ports[i])))
            setattr(self, 'mq%s'%i, 
                MessageQueue(self.nodes[:], self.nodes[i], getattr(self, 'rpc_server%s'%i))
            )
            getattr(self, 'mq%s'%i).init_store(self.dirs[2*i], self.dirs[2*i+1])
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
           
        mq.put(data)
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
        
    def testAddOrRemoveNode(self):
        mq = self.mq0
        data = [str(i) for i in range(100)]
        
        mq.put(data)
        self.mq2.shutdown()
        self.assertEqual(len(self.nodes), 3)
        self.mq0.remove_node(self.nodes[2])
        self.assertEqual(len(self.nodes), 3)
        self.mq1.remove_node(self.nodes[2])
        
        gets = []
        while True:
            get = mq.get()
            if get is None:
                break
            gets.append(get)
          
        self.assertEqual(sorted(data), sorted(gets))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()