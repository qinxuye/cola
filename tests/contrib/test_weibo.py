'''
Created on 2013-6-10

@author: Chine
'''
import unittest
import time

from cola.core.opener import MechanizeOpener

from contrib.weibo import login_hook
from contrib.weibo.parsers import MicroBlogParser, ForwardCommentLikeParser, \
                                    UserInfoParser, UserFriendParser
from contrib.weibo.conf import user_config
from contrib.weibo.bundle import WeiboUserBundle

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
        _, bundles = parser.parse()
           
        self.assertEqual(len(bundles), 0)
            
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertEqual(len(user['statuses']), 15)
        
    def testMicroBlogForwardsParser(self):
        test_url = 'http://weibo.com/aj/mblog/info/big?id=3596988739933218&_t=0&__rnd=1373094212593'
        parser = ForwardCommentLikeParser(opener=self.opener,
                                          url=test_url,
                                          bundle=self.bundle)
        urls, _ = parser.parse()
        
        self.assertEqual(len(urls), 1)
        
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertLessEqual(len(user['statuses'][0]['forwards']), 20)
        self.assertGreater(len(user['statuses'][0]['forwards']), 0)
        
        parser.parse(urls[0])
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertLessEqual(len(user['statuses'][0]['forwards']), 40)
        self.assertGreater(len(user['statuses'][0]['forwards']), 20)
        self.assertNotEqual(user['statuses'][0]['forwards'][0], 
                            user['statuses'][0]['forwards'][20])
        
    def testMicroBlogForwardTimeParser(self):
        test_url = 'http://weibo.com/aj/mblog/info/big?_t=0&id=3600369441313426&__rnd=1373977781515'
        parser = ForwardCommentLikeParser(opener=self.opener,
                                          url=test_url,
                                          bundle=self.bundle)
        parser.parse()
        
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertGreater(len(user['statuses'][0]['forwards']), 0)
        
    def testMicroBlogLikesParser(self):
        test_url = 'http://weibo.com/aj/like/big?mid=3599246068109415&_t=0&__rnd=1373634556882'
        parser = ForwardCommentLikeParser(opener=self.opener,
                                          url=test_url,
                                          bundle=self.bundle)
        urls, _ = parser.parse()
        
        self.assertEqual(len(urls), 1)
        
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertEqual(len(user['statuses'][0]['likes']), 30)
  
    def testUserInfoParser(self):
        test_url = 'http://weibo.com/%s/info' % self.test_uid
        parser = UserInfoParser(opener=self.opener,
                                url=test_url,
                                bundle=self.bundle)
        parser.parse()
            
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertTrue('info' in user)
         
    def testUserInfoParserForSite(self):
        test_uid = '2733272463'
        test_url = 'http://weibo.com/%s/info' % test_uid
        bundle = WeiboUserBundle(test_uid)
        parser = UserInfoParser(opener=self.opener,
                                url=test_url,
                                bundle=bundle)
        parser.parse()
         
    def testFriendParser(self):
        test_url = 'http://weibo.com/%s/follow' % self.test_uid
        parser = UserFriendParser(opener=self.opener,
                                  url=test_url,
                                  bundle=self.bundle)
        urls, bundles = parser.parse()
        self.assertEqual(len(urls), 1)
        self.assertGreater(bundles, 0)
          
        user = self.collection.find_one({'uid': self.test_uid})
        self.assertEqual(len(bundles), len(user['follows']))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()