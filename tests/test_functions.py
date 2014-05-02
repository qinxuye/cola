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

Created on 2014-5-2

@author: chine
'''
import unittest
import random
import tempfile
import threading
import shutil

from cola.context import Settings
from cola.core.rpc import ColaRPCServer
from cola.functions.budget import BudgetApplyClient, BudgetApplyServer

class Test(unittest.TestCase):


    def setUp(self):
        port = random.randint(10000, 30000)
        self.rpc_server = ColaRPCServer(('localhost', port))
        thd = threading.Thread(target=self.rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        self.dir_ = tempfile.mkdtemp()
        self.serv = BudgetApplyServer(self.dir_, Settings(), rpc_server=self.rpc_server)
        self.cli1 = BudgetApplyClient(self.serv)
        self.cli2 = BudgetApplyClient('localhost:%s'%port)

    def tearDown(self):
        try:
            self.rpc_server.shutdown()
            self.serv.shutdown()
        finally:
            shutil.rmtree(self.dir_)

    def testBudgetApply(self):
        self.serv.set_budgets(90)
        self.assertEqual(self.cli1.apply(50), 50)
        self.assertEqual(self.cli2.apply(50), 40)
        
        self.cli1.finish(50)
        self.assertEqual(50, self.serv.finished)
        self.cli2.finish(50)
        self.assertEqual(90, self.serv.finished)
        
        self.cli1.error(10)
        self.assertEqual(90, self.serv.applied)
        self.serv.finished = 0
        self.cli2.error(10)
        self.assertEqual(80, self.serv.applied)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()