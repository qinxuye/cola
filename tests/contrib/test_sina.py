'''
Created on 2013-6-10

@author: Chine
'''
import unittest
import time

from cola.core.opener import MechanizeOpener

from contrib.sina import login_hook
from contrib.sina.parsers import MicroBlogParser, UserInfoParser
from contrib.sina.conf import user_config
from contrib.sina.bundle import WeiboUserBundle

from pymongo import Connection

class Test(unittest.TestCase):


    def setUp(self):
        self.test_uid = '1784725941'
        self.bundle = WeiboUserBundle(self.test_uid)
        self.opener = MechanizeOpener()
        
        self.conn = Connection()
        self.db = self.conn[getattr(user_config.job, 'db')]
        self.collection = self.db.weibo_user
        
        assert len(user_config.job['login']) > 0
        
        login_hook(self.opener, **user_config.job['login'][0])

    def tearDown(self):
        self.collection.remove({'uid': self.test_uid})
        self.conn.close()
        
    def testMicroBlogParser(self):
        test_url = 'http://weibo.com/aj/mblog/mbloglist?uid=%s&_k=%s' % (
            self.test_uid,
            int(time.time() * (10**6))
        )
        parser = MicroBlogParser(opener=self.opener, 
                                 url=test_url, 
                                 bundle=self.bundle)
        urls, bundles = parser.parse()
         
        self.assertEqual(len(urls), 1)
        self.assertEqual(len(bundles), 0)
         
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertEqual(len(user['statuses']), 15)

    def testUserInfoParser(self):
        test_url = 'http://weibo.com/%s/info' % self.test_uid
        parser = UserInfoParser(opener=self.opener,
                                url=test_url,
                                bundle=self.bundle)
        parser.parse()
         
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertTrue('info' in user)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()