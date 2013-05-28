#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-17

@author: Chine
'''
import unittest

from cola.core.opener import BuiltinOpener, MechanizeOpener


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

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()