#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-16

@author: Chine
'''
import unittest

from cola.core.opener import MechanizeOpener
from cola.core.extractor.preprocess import PreProcessor
from cola.core.extractor import Extractor

class Test(unittest.TestCase):
    
    def setUp(self):
        self.base_url = 'http://zhidao.baidu.com'
        self.url = 'http://zhidao.baidu.com/question/559110619.html'
        self.html = MechanizeOpener().open(self.url)

    def testPreprocess(self):
        pre_process = PreProcessor(self.html, self.base_url)
        title, body = pre_process.process()
         
        self.assertTrue(u'百度' in title)
        self.assertGreater(len(body.text), 0)

    def testExtractor(self):
        extractor = Extractor(self.html, self.base_url)
        content = extractor.extract()
        self.assertGreater(len(content), 0)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testExtractor']
    unittest.main()