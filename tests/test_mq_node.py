'''
Created on 2013-5-25

@author: Chine
'''
import unittest
import tempfile
import os
import random
import shutil

from cola.core.mq.node import Node, NodeNoSpaceForPut


class Test(unittest.TestCase):


    def setUp(self):
        self.dir_ = tempfile.mkdtemp()
        self.node = Node(self.dir_)

    def tearDown(self):
        self.node.shutdown()
        shutil.rmtree(self.dir_)

    def testLockExists(self):
        self.assertTrue(os.path.exists(os.path.join(self.dir_, 'lock')))
          
    def testPutGet(self):
        get_num = lambda: random.randint(10000, 20000)
          
        num1 = get_num()
        self.assertEqual(self.node.put(str(num1)), str(num1))
        num2 = get_num()
        self.assertEqual(self.node.put(str(num2)), str(num2))
          
        self.assertEqual(self.node.get(), str(num1))
        self.assertEqual(self.node.get(), str(num2))
        
    def testBatchPutGet(self):
        self.node.shutdown()
        
        size = 50
        batch1 = ['1' * 20, '2' * 20]
        batch2 = ['3' * 20, '4' * 20]
        
        self.node = Node(self.dir_, size)
        
        self.assertEqual(self.node.put(batch1), batch1)
        self.assertEqual(self.node.put(batch2), batch2)
        
        self.assertEqual(len(self.node.map_files), 2)
        
        gets = sorted([self.node.get() for _ in range(4)])
        res = batch1
        res.extend(batch2)
        self.assertEqual(gets, res)
        
        self.node.put('5' * 20)
        self.assertEqual(self.node.get(), '5' * 20)
        
        self.node.put('6' * 20)
        
        self.node.merge()
        self.assertEqual(len(self.node.map_files), 1)
        
        self.assertEqual(self.node.get(), '6' * 20)
        self.assertEqual(self.node.get(), None)
        
        self.assertRaises(NodeNoSpaceForPut, lambda: self.node.put('7' * 100))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()