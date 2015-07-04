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

Created on 2014-6-19

@author: chine
'''

import unittest
import tempfile
import os
import shutil
import errno
import time

import yaml

from cola.core.utils import import_job_desc
from cola.core.zip import ZipHandler
from cola.context import Context

class Test(unittest.TestCase):


    def setUp(self):
        self.working_dir = tempfile.mkdtemp() 
        self.job_dir = os.path.join(self.working_dir, 'master', 'jobs')
        self.zip_dir = os.path.join(self.working_dir, 'master', 'zip')
        
        if not os.path.exists(self.zip_dir):
            os.makedirs(self.zip_dir)
        
        wiki_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'app', 'wiki')
        try:
            shutil.copytree(wiki_path, os.path.join(self.job_dir, 'wiki'))
        except OSError, e:
            if e.errno == errno.ENOTDIR:
                shutil.copy(wiki_path, os.path.join(self.job_dir, 'wiki'))
            else:
                raise
        
        self.job_name = import_job_desc(wiki_path).uniq_name
        old_wiki_path = os.path.join(self.job_dir, 'wiki')
        new_wiki_path = os.path.join(self.job_dir, self.job_name)
        os.rename(old_wiki_path, new_wiki_path)
        
        ZipHandler.compress(os.path.join(self.zip_dir, self.job_name+'.zip'), 
                            new_wiki_path)
        
        config_file = os.path.join(new_wiki_path, 'wiki.yaml')
        try:
            os.remove(os.path.join(new_wiki_path, 'test.yaml'))
        except:
            pass
        
        with open(config_file) as f:
            yaml_obj = yaml.load(f)
            yaml_obj['job']['size'] = 5
            yaml_obj['job']['instances'] = 1
            yaml_obj['job']['priorities'] = 1
        with open(config_file, 'w') as f:
            yaml.dump(yaml_obj, f)

    def tearDown(self):
        try:
            shutil.rmtree(self.working_dir)
        except:
            pass

    def testMasterWorker(self):
        ctx = Context(is_master=True, master_addr='127.0.0.1', 
                      working_dir=self.working_dir)
        master = ctx.start_master()
        ctx.start_worker()
        
        master.run_job(self.job_name, wait_for_workers=True)
        
        while master.has_running_jobs():
            time.sleep(5)
            
        master.shutdown()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()