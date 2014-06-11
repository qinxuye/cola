'''
Created on 2014-6-11

@author: chine
'''
import unittest
import tempfile
import shutil
import random
import pickle

from cola.context import ContextManager
from cola.core.mq import MessageQueueRPCProxy
from cola.core.rpc import ThreadedColaRPCServer, client_call
from cola.core.unit import Url

class Test(unittest.TestCase):


    def setUp(self):
        self.dir_ = tempfile.mkdtemp()
        self.addr = '127.0.0.1'
        self.addrs = [self.addr, ]
        
        self.manager = ContextManager()
        self.manager.start()
        
        port = random.randint(10000, 30000)
        self.loc = '%s:%s' % (self.addr, port)
        self.rpc_server = ThreadedColaRPCServer((self.addr, port))
        
    def tearDown(self):
        try:
            self.manager.shutdown()
            self.rpc_server.shutdown()
        finally:
            shutil.rmtree(self.dir_)


    def testMqProxy(self):
        self.mq = self.manager.mq(self.dir_, None, self.addr, self.addrs,
                                  copies=0, n_priorities=1)
        self.proxy = MessageQueueRPCProxy(self.mq.get_connection(), self.rpc_server)
        
        try:
            test_obj = Url('http://qinxuye.me')
            client_call(self.loc, 'put', pickle.dumps(test_obj))
            self.assertEqual(pickle.loads(client_call(self.loc, 'get')), test_obj)
        finally:
            self.mq.shutdown()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()