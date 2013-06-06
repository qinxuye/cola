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

Created on 2013-6-6

@author: Chine
'''
import unittest
import tempfile
import shutil
import os

from cola.core.zip import ZipHandler

class Test(unittest.TestCase):


    def setUp(self):
        self.f = tempfile.mkdtemp()
        self.content = 'This is a test file!'
        
        self.src_dir = os.path.join(self.f, 'compress')
        os.mkdir(self.src_dir)
        with open(os.path.join(self.src_dir, '1.txt'), 'w') as fp:
            fp.write(self.content)
        dir1 = os.path.join(self.src_dir, 'dir1')
        os.mkdir(dir1)
        with open(os.path.join(dir1, '2.txt'), 'w') as fp:
            fp.write(self.content)
            
        self.dest_dir = os.path.join(self.f, 'uncompress')

    def tearDown(self):
        shutil.rmtree(self.f)


    def testZip(self):
        zip_file = os.path.join(self.f, 'test.zip')
        
        ZipHandler.compress(zip_file, self.src_dir)
        ZipHandler.uncompress(zip_file, self.dest_dir)
        
        dir_ = os.path.join(self.dest_dir, 'compress')
        self.assertTrue(os.path.exists(dir_))
        
        with open(os.path.join(dir_, '1.txt')) as fp:
            self.assertEqual(fp.read(), self.content)
            
        dir1 = os.path.join(dir_, 'dir1')
        self.assertTrue(os.path.exists(dir1))
        
        with open(os.path.join(dir1, '2.txt')) as fp:
            self.assertEqual(fp.read(), self.content)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()