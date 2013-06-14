'''
Created on 2013-6-14

@author: Chine
'''
import unittest
import tempfile
import shutil
import os

from cola.job import Job
from cola.core.urls import UrlPatterns
from cola.core.opener import BuiltinOpener
from cola.core.utils import get_ip
from cola.master.loader import MasterJobLoader
from cola.worker.loader import WorkerJobLoader

class Test(unittest.TestCase):


    def setUp(self):
        self.job = Job('test job', UrlPatterns(), BuiltinOpener, [])
        self.root = tempfile.mkdtemp()
        
        master_root = os.path.join(self.root, 'master')
        worker_root = os.path.join(self.root, 'worker')
        os.makedirs(master_root)
        os.makedirs(worker_root)
        
        node = '%s:%s' % (get_ip(), self.job.context.job.port)
        nodes = [node]
        master = '%s:%s' % (get_ip(), self.job.context.job.master_port)
        
        
        self.master_loader = MasterJobLoader(self.job, master_root, nodes)
        self.worker_loader = WorkerJobLoader(self.job, worker_root, master)

    def tearDown(self):
        try:
            self.worker_loader.finish()
            self.master_loader.finish()
        finally:
            shutil.rmtree(self.root)


    def testLog(self):
        self.worker_loader.logger.info('here is the msg')
        self.worker_loader.logger.error('sth error')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()