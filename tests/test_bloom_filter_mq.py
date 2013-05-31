#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-1

@author: Chine
'''

import unittest
import tempfile
import shutil
import os

from cola.core.mq.node import Node
from cola.core.bloomfilter import FileBloomFilter

class Test(unittest.TestCase):


    def setUp(self):
        self.dir_ = tempfile.mkdtemp()
        self.node_dir = os.path.join(self.dir_, 'node')
        os.mkdir(self.node_dir)
        bloom_filter_hook = FileBloomFilter(
            os.path.join(self.dir_, 'bloomfilter'), 10
        )
        self.node = Node(self.node_dir, verify_exists_hook=bloom_filter_hook)

    def tearDown(self):
        self.node.shutdown()
        shutil.rmtree(self.dir_)
          
    def testPutGet(self):
        num = str(12345)
        
        self.assertEqual(self.node.put(num), num)
        self.assertEqual(self.node.put(num), '')
        
        num2 = str(67890)
        nums = [num, num2]
        self.assertEqual(self.node.put(nums), [num2])
        
        self.node.shutdown()
        self.assertGreater(os.path.getsize(os.path.join(self.dir_, 'bloomfilter')), 0)
        
        bloom_filter_hook = FileBloomFilter(
            os.path.join(self.dir_, 'bloomfilter'), 5
        )
        self.node = Node(self.node_dir, verify_exists_hook=bloom_filter_hook)
        
        num3 = str(13579)
        nums = [num, num2, num3]
        self.assertEqual(self.node.put(nums), [num3])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()