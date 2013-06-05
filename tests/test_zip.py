'''
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