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

Created on 2013-6-11

@author: Chine
'''
import unittest
import threading

from cola.core.logs import get_logger, LogRecordSocketReceiver

class Test(unittest.TestCase):


    def setUp(self):
        self.client_logger = get_logger(name='cola_test_client', server='localhost')
        self.server_logger = get_logger(name='cola_test_server')
        
        self.log_server = LogRecordSocketReceiver(logger=self.server_logger)
        threading.Thread(target=self.log_server.serve_forever).start()

    def tearDown(self):
        self.log_server.shutdown()
        self.log_server.stop()

    def testLog(self):
        self.client_logger.error('Sth happens here')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLog']
    unittest.main()