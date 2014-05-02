#!/usr/bin/env python
# -*- coding: utf-8 -*-
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