'''
Created on 2013-5-23

@author: Chine
'''
from __future__ import with_statement
import unittest
import xmlrpclib
import random
import socket
import threading
import signal

from cola.core.rpc import ColaRPCServer

def test_plus_one(num):
    return num + 1

class Test(unittest.TestCase):
    
    def client_call(self):
        server = xmlrpclib.ServerProxy('http://localhost:11103')
        num = random.randint(0, 100)
        plus_one_num = server.test_plus_one(num)
        self.assertEqual(plus_one_num, num + 1)
        
    def start_server(self):
        self.server = ColaRPCServer(('localhost', 11103))
        self.server.register_function(test_plus_one)
        self.server.serve_forever()

    def setUp(self):
        self.server_run = threading.Thread(target=self.start_server)
            
    def testRPC(self):
        self.server_run.start()
        self.client_call()
        self.server.shutdown()
        del self.server
        with self.assertRaises(socket.error):
            self.client_call()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()