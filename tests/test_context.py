'''
Created on 2013-5-25

@author: Chine
'''
import unittest
try:
    from StringIO import StringIO
except ImportError:
    from cStringIO import StringIO

from cola.core.config import Config
from cola.job.context import Context

class Test(unittest.TestCase):


    def setUp(self):
        self.simulate_user_conf = Config(StringIO('name: cola-unittest'))


    def testContext(self):
        context = Context(user_conf=self.simulate_user_conf, 
                          description='This is a just unittest')
        self.assertEqual(context.name, 'cola-unittest')
        self.assertEqual(context.description, 'This is a just unittest')
        self.assertEqual(context.job.db, 'cola')
        self.assertEqual(context.job.port, 12103)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()