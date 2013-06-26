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

Created on 2013-6-27

@author: Chine
'''

from cola.core.errors import DependencyNotInstalledError

from conf import mongo_host, mongo_port, db_name

try:
    from mongoengine import connect, Document, Q, DoesNotExist, \
                            StringField, DateTimeField, IntField
except ImportError:
    raise DependencyNotInstalledError('mongoengine')

connect(db_name, host=mongo_host, port=mongo_port)

DoesNotExist = DoesNotExist
Q = Q

class MicroBlog(Document):
    content = StringField()
    forward = StringField()
    created = DateTimeField()
    
    likes = IntField()
    forwards = IntField()
    comments = IntField()
    
    mid = StringField(required=True)
    keyword = StringField(required=True)