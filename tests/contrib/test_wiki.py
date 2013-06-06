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
            lang = url.strip('http://').split('.', 1)[0]
            self.assertEqual(parser.title, 'Python '+lang)
            self.assertGreater(len(parser.content), 0)
            self.assertTrue(isinstance(parser.last_update, datetime))
            
            self.assertIsNotNone(url_patterns.get_parser(url))
            
        parser = WikiParser()
        url = 'http://en.wikipedia.org/wiki/Python'
        parser.parse(url)
        
        from pymongo import Connection
        conn = Connection(mongo_host, mongo_port)
        db = getattr(conn, db_name)
        wiki = db.wiki_document.find_one({'title': 'Python en'})
        self.assertIsNotNone(wiki)
        
        db.wiki_document.remove({'title': 'Python en'})

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()