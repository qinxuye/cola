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

Created on 2013-5-21

@author: Chine
'''
import unittest
import pickle

from cola.core.config import PropertyObject, main_conf

class Test(unittest.TestCase):


    def setUp(self):
        self.obj = PropertyObject({
            'name': 'cola',
            'list': [
                { 'count': 1 },
                { 'count': 2 },
            ]
        })


    def testPropertyObject(self):
        assert 'name' in self.obj
        assert self.obj['name'] == 'cola'
        assert self.obj.name == 'cola'
        assert isinstance(self.obj.list, list)
        assert self.obj.list[0].count == 1
        
    def testPickle(self):
        c = pickle.dumps(main_conf)
        new_conf = pickle.loads(c)
        self.assertEqual(new_conf.master.port, 11103)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()