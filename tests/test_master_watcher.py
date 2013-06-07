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

Created on 2013-6-7

@author: Chine
'''
import unittest
import tempfile
import os
import shutil
import threading

from cola.core.zip import ZipHandler
from cola.core.utils import root_dir
from cola.core.rpc import ColaRPCServer
from cola.master.watcher import MasterWatcher
from cola.job.conf import main_conf

class Test(unittest.TestCase):


    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.zip_dir = os.path.join(self.dir, 'zip')
        if not os.path.exists(self.zip_dir):
            os.mkdir(self.zip_dir)
        self.job_dir = os.path.join(self.dir, 'job')
        if not os.path.exists(self.job_dir):
            os.mkdir(self.job_dir)
            
        zip_file = os.path.join(self.zip_dir, 'wiki.zip')
        src_dir = os.path.join(root_dir(), 'contrib', 'wiki')
        self.zip_file = ZipHandler.compress(zip_file, src_dir, type_filters=('pyc', ))
        
        self.rpc_server = ColaRPCServer(('localhost', main_conf.master.port))
        self.master_watcher = MasterWatcher(self.rpc_server, self.zip_dir, self.job_dir)
        
        thd = threading.Thread(target=self.rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        
    def tearDown(self):
        self.rpc_server.shutdown()
        shutil.rmtree(self.dir)


    def testMasterWatcher(self):
        self.master_watcher.start_job(self.zip_file)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()