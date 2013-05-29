'''
Created on 2013-5-29

@author: Chine
'''
import unittest
from datetime import datetime

from contrib.wiki import WikiParser, url_patterns

class FakeWikiParser(WikiParser):
    def store(self, title, content, last_update):
        return title, content, last_update

class Test(unittest.TestCase):


    def testWikiParser(self):
        parser = FakeWikiParser()
        
        for url in ('http://en.wikipedia.org/wiki/Python',
                    'http://zh.wikipedia.org/wiki/Python'):
            title, content, last_update = parser.parse(url)
            self.assertEqual(title, 'Python')
            self.assertGreater(len(content), 0)
            self.assertTrue(isinstance(last_update, datetime))
            
            self.assertIsNotNone(url_patterns.get_parser(url))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()