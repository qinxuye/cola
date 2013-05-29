'''
Created on 2013-5-29

@author: Chine
'''
import unittest
from datetime import datetime

from contrib.wiki import WikiParser, url_patterns, \
                         mongo_host, mongo_port, db_name

class FakeWikiParser(WikiParser):
    def store(self, title, content, last_update):
        self.title, self.content, self.last_update = title, content, last_update

class Test(unittest.TestCase):


    def testWikiParser(self):
        parser = FakeWikiParser()
        
        for url in ('http://en.wikipedia.org/wiki/Python',
                    'http://zh.wikipedia.org/wiki/Python'):
            parser.parse(url)
            self.assertEqual(parser.title, 'Python')
            self.assertGreater(len(parser.content), 0)
            self.assertTrue(isinstance(parser.last_update, datetime))
            
            self.assertIsNotNone(url_patterns.get_parser(url))
            
        parser = WikiParser()
        url = 'http://en.wikipedia.org/wiki/Python'
        parser.parse(url)
        
        from pymongo import Connection
        conn = Connection(mongo_host, mongo_port)
        db = getattr(conn, db_name)
        wiki = db.wiki_document.find_one({'title': 'Python'})
        self.assertIsNotNone(wiki)
        
        db.wiki_document.remove({'title': 'Python'})

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()