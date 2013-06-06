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