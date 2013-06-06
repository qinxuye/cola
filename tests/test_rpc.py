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

Created on 2013-5-23

@author: Chine
'''
from __future__ import with_statement
import unittest
import xmlrpclib
import random
import socket
import threading

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