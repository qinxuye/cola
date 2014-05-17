#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

Created on 2014-5-17

@author: chine
'''

class Executor(object):
    def __init__(self, unit, mq, stopped, nonsuspend, counter):
        self.unit = unit
        self.mq = mq
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        self.counter = counter
        
    def execute(self):
        raise NotImplementedError

class UrlExecutor(Executor):
    def __init__(self, url, mq, stopped, nonsuspend, counter):
        super(UrlExecutor, self).__init__(url, mq, stopped, nonsuspend, counter)
        self.url = url

class BundleExecutor(Executor):
    def __init__(self, bundle, mq, stopped, nonsuspend, counter, max_sec):
        super(BundleExecutor, self).__init__(bundle, mq, stopped, 
                                             nonsuspend, counter)
        self.bundle = bundle
        self.max_sec = max_sec