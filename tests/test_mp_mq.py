'''
Created on 2014-6-11

@author: chine
'''
import unittest
import tempfile
import shutil

from cola.core.mq import MessageQueue, MpMessageQueueClient
from cola.core.unit import Url

class Test(unittest.TestCase):


    def setUp(self):
        self.dir_ = tempfile.mkdtemp()
        self.addr = '127.0.0.1'
        self.addrs = [self.addr, ]
        
    def tearDown(self):
        try:
            self.mq.shutdown()
        finally:
            shutil.rmtree(self.dir_)


    def testMqProxy(self):
        self.mq = MessageQueue(self.dir_, None, self.addr, self.addrs,
                                  copies=0, n_priorities=1)
        self.proxy = MpMessageQueueClient(self.mq.new_connection('0'))
        
        try:
            test_obj = Url('http://qinxuye.me')
            self.proxy.put(test_obj, )
            self.assertEqual(self.proxy.get(), test_obj)
        finally:
            self.mq.shutdown()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()