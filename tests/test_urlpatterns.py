'''
Created on 2013-5-29

@author: Chine
'''
import unittest

from cola.core.parsers import Parser
from cola.core.urls import Url, UrlPatterns

class FakeParser(Parser):
    pass
    
class Test(unittest.TestCase):


    def testUrlPatterns(self):
        url_patterns = UrlPatterns(
            Url(r'^http://zh.wikipedia.org/wiki/[^FILE][^/]+$', 'wiki_item', FakeParser)
        )
        
        urls = ['http://zh.wikipedia.org/wiki/%E6%97%A0%E6%95%8C%E8%88%B0%E9%98%9F',
                ]
        self.assertTrue(list(url_patterns.matches(urls)), urls)
        self.assertEqual(url_patterns.get_parser(urls[0]), FakeParser)
        
        self.assertFalse(Url('^http://zh.wikipedia.org/wiki/[^FILE][^/]+$', None, None).match('http://zh.wikipedia.org/wiki/File:Flag_of_Cross_of_Burgundy.svg'))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testUrlPatterns']
    unittest.main()