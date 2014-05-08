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
from cola.functions.counter import CounterClient, CounterServer
from cola.functions.speed import SpeedControlClient, SpeedControlServer

class Test(unittest.TestCase):


    def setUp(self):
        self.port = random.randint(10000, 30000)
        self.addr = 'localhost:%s' % self.port
        self.rpc_server = ColaRPCServer(('localhost', self.port))
        thd = threading.Thread(target=self.rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        self.dir_ = tempfile.mkdtemp()

    def tearDown(self):
        try:
            self.rpc_server.shutdown()
        finally:
            shutil.rmtree(self.dir_)

    def testBudgetApply(self):
        self.serv = BudgetApplyServer(self.dir_, Settings(), rpc_server=self.rpc_server)
        self.cli1 = BudgetApplyClient(self.serv)
        self.cli2 = BudgetApplyClient('localhost:%s'%self.port)
           
        try:
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
        finally:
            self.serv.shutdown()
             
    def testCounter(self):
        self.serv = CounterServer(self.dir_, Settings(),
                                  rpc_server=self.rpc_server)
        self.cli1 = CounterClient(self.serv)
        self.cli2 = CounterClient('localhost:%s'%self.port)
         
        try:
            self.cli1.global_inc('pages', 10)
            self.cli2.global_inc('pages', 2)
            self.assertEqual(self.cli1.get_global_inc('pages'), 10)
            self.assertEqual(self.cli2.get_global_inc('pages'), 2)
            self.assertEqual(self.serv.inc_counter.get('global', 'pages', 0), 0)
             
            self.cli1.sync()
            self.cli2.sync()
            self.assertEqual(self.cli1.get_global_inc('pages'), None)
            self.assertEqual(self.cli2.get_global_inc('pages'), None)
            self.assertEqual(self.serv.inc_counter.get('global', 'pages'), 12)
             
            self.cli1.local_inc(self.addr, 0, 'pages', 100)
            self.assertEqual(self.cli1.get_local_inc(self.addr, 0, 'pages'), 100)
            self.cli1.sync()
            self.assertEqual(self.serv.inc_counter.get('%s#%s'%(self.addr, 0), 'pages'), 100)
             
            self.cli1.global_acc('normal', 100)
            self.cli2.global_acc('normal', 'test')
            self.assertEqual(self.cli1.get_global_acc('normal'), [100,])
            self.assertEqual(self.cli2.get_global_acc('normal'), ['test',])
             
            self.cli1.sync()
            self.cli2.sync()
            self.assertEqual(self.cli1.get_global_acc('normal'), None)
            self.assertEqual(self.cli2.get_global_acc('normal'), None)
            self.assertEqual(self.serv.acc_counter.get('global', 'normal'), [100, 'test'])
             
            self.cli2.local_acc(self.addr, 1, 'normal', 100)
            self.assertEqual(self.cli2.get_local_acc(self.addr, 1, 'normal'), [100, ])
            self.cli2.sync()
            self.assertEqual(self.serv.acc_counter.get('%s#%s'%(self.addr, 1), 'normal'), [100, ])
        finally:
            self.serv.shutdown()
            
    def testSpeedControl(self):
        settings = Settings()
        counter_server = CounterServer(self.dir_, settings)
        self.serv = SpeedControlServer(self.dir_, settings, rpc_server=self.rpc_server,
                                       counter_server=counter_server)
        self.cli = SpeedControlClient(self.serv, 'localhost', 0)
        
        self.assertEqual(self.cli.require(10), (10, 0))
        
        self.serv.set_speed(100)
        self.assertEqual(self.cli.require(100), (100, 0.5))
        self.assertEqual(self.cli.require(1), (0, 0.5))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()