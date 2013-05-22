'''
Created on 2013-5-22

@author: Chine
'''

class Context(object):
    def __init__(self, config, master, workers):
        self.config = config
        self.master = master
        self.workers = workers