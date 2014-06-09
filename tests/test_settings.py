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

Created on 2013-5-25

@author: Chine
'''
import unittest
try:
    from StringIO import StringIO
except ImportError:
    from cStringIO import StringIO

from cola.core.config import Config
from cola.settings import Settings

class Test(unittest.TestCase):


    def setUp(self):
        self.simulate_user_conf = Config(StringIO('name: cola-unittest'))


    def testContext(self):
        settings = Settings(user_conf=self.simulate_user_conf, 
                           description='This is a just unittest')
        self.assertEqual(settings.name, 'cola-unittest')
        self.assertEqual(settings.description, 'This is a just unittest')
        self.assertEqual(settings.job.db, 'cola')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()