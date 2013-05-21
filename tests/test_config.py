'''
Created on 2013-5-21

@author: Chine
'''
import unittest

from cola.core.config import PropertyObject

class Test(unittest.TestCase):


    def setUp(self):
        self.obj = PropertyObject({
            'name': 'cola',
            'list': [
                { 'count': 1 },
                { 'count': 2 },
            ]
        })


    def testPropertyObject(self):
        assert 'name' in self.obj
        assert self.obj['name'] == 'cola'
        assert self.obj.name == 'cola'
        assert isinstance(self.obj.list, list)
        assert self.obj.list[0].count == 1


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()