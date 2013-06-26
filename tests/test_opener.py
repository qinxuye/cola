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

Created on 2013-5-17

@author: Chine
'''
import unittest

from cola.core.opener import BuiltinOpener, MechanizeOpener, \
                            SpynnerOpener

class Test(unittest.TestCase):


    def testBuiltinOpener(self):
        opener = BuiltinOpener()
        assert 'baidu' in opener.open('http://www.baidu.com')
          
    def testMechanizeOpener(self):
        test_url = 'http://www.baidu.com'
        opener = MechanizeOpener()
          
        assert 'baidu' in opener.open(test_url)
          
        br = opener.browse_open(test_url)
        assert u'百度' in br.title()
        assert 'baidu' in br.response().read()
        
    def testSpynnerOpener(self):
        test_url = 'http://s.weibo.com/'
        opener = SpynnerOpener()
        
        br = opener.spynner_open(test_url)
        br.wk_fill('input.searchInp_form', u'超级月亮')
        br.click('a.searchBtn')
        br.wait_for_content(lambda br: 'feed_lists W_linka W_texta' in br.html)
        
        self.assertIn(u'超级月亮', br.html)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()