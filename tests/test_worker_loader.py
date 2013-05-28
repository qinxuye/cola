#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-28

@author: Chine
'''

try:
    from StringIO import StringIO
except ImportError:
    from cStringIO import StringIO
import unittest
import tempfile
import shutil
import threading
import urlparse

from cola.core.opener import MechanizeOpener
from cola.core.parsers import Parser
from cola.core.urls import UrlPatterns, Url
from cola.core.config import Config
from cola.core.rpc import ColaRPCServer
from cola.job import Job
from cola.worker.loader import JobLoader

f = StringIO()
sep = '---------------------%^&*()---------------------'

class FakeWikiParser(Parser):
    def parse(self, url=None):
        url = url or self.url
        
        def _is_same(out_url):
            return out_url.rsplit('#', 1)[0] == url
        
        opener = self.opener()
        br = opener.browse_open(url)
        f.write(br.response().read())
        f.write(sep)
        
        links = []
        for link in br.links():
            if link.url.startswith('http://'):
                out_url = link.url
                if not _is_same(out_url):
                    links.append(out_url)
            else:
                out_url = urlparse.urljoin(link.base_url, link.url)
                if not _is_same(out_url):
                    links.append(out_url)
        return links
        
user_conf = '''job:
  db: cola
  mode: url
  size: 10
  limit: 0
  master_port: 12102
  port: 12103
  instances: 1'''

class Test(unittest.TestCase):


    def setUp(self):
        url_patterns = UrlPatterns(
            Url(r'^http://zh.wikipedia.org/wiki/[^(:|/)]+$', 'wiki_item', FakeWikiParser)
        )
        fake_user_conf = Config(StringIO(user_conf))
        
        self.dir = tempfile.mkdtemp()
        
        self.job = Job('fake wiki crawler', url_patterns, MechanizeOpener, 
                       ['http://zh.wikipedia.org/wiki/%E6%97%A0%E6%95%8C%E8%88%B0%E9%98%9F', ],
                       user_conf=fake_user_conf)
        
        local_node = 'localhost:%s' % self.job.context.job.port
        nodes = [local_node, ]
        
        self.rpc_server = ColaRPCServer(('localhost', self.job.context.job.port))
        self.loader = JobLoader(self.job)
        self.loader.init_mq(self.rpc_server, nodes, local_node, self.dir)
        
        thd = threading.Thread(target=self.rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()

    def tearDown(self):
        try:
            self.loader.finish()
            self.rpc_server.shutdown()
        finally:
            shutil.rmtree(self.dir)


    def testJobLoader(self):
        self.assertEqual(len(self.job.starts), 1)
        
        self.loader.mq.put(self.job.starts)
        self.assertEqual(self.loader.mq.get(), self.job.starts[0])
        
        # put starts into mq again
        self.loader.mq.put(self.job.starts)
        self.loader.run()
         
        self.assertEqual(len(f.getvalue().strip(sep).split(sep)), 10)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()