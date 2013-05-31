'''
Created on 2013-5-31

@author: Chine
'''
import unittest
import random

from cola.core.bloomfilter import BloomFilter


class Test(unittest.TestCase):

    def get_random(self):
        size = random.randint(2, 10)
        return ''.join([str(random.randint(1, 9)) for _ in range(size)])

    def testBloomFilter(self):
        bf = BloomFilter(capacity=10000)
        self.assertFalse('apple' in bf)
        bf.add('apple')
        self.assertTrue('apple' in bf)
        self.assertFalse('banana' in bf)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testBloomFilter']
    unittest.main()