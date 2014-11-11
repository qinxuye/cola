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

Created on 2013-6-9

@author: Chine
'''

from cola.core.errors import DependencyNotInstalledError

from conf import mongo_host, mongo_port, db_name, shard_key

try:
    from mongoengine import connect, Document, EmbeddedDocument, \
                            DoesNotExist, Q, \
                            StringField, DateTimeField, EmailField, \
                            BooleanField, URLField, IntField, FloatField, \
                            ListField, EmbeddedDocumentField, \
                            ValidationError
except ImportError:
    raise DependencyNotInstalledError('mongoengine')

connect(db_name, host=mongo_host, port=mongo_port)

DoesNotExist = DoesNotExist
Q = Q
ValidationError = ValidationError

class Forward(EmbeddedDocument):
    mid = StringField(required=True)
    uid = StringField(required=True)
    avatar = URLField()
    content = StringField()
    created = DateTimeField()

class Comment(EmbeddedDocument):
    uid = StringField(required=True)
    avatar = URLField()
    content = StringField()
    created = DateTimeField()
    
class Like(EmbeddedDocument):
    uid = StringField(required=True)
    avatar = URLField()
    
class Geo(EmbeddedDocument):
    longtitude = FloatField()
    latitude = FloatField()
    location = StringField()

class MicroBlog(Document):
    mid = StringField(required=True)
    uid = StringField(required=True)
    content = StringField()
    omid = StringField()
    forward = StringField()
    created = DateTimeField()
    geo = EmbeddedDocumentField(Geo)
    
    n_likes = IntField()
    likes = ListField(EmbeddedDocumentField(Like))
    n_forwards = IntField()
    forwards = ListField(EmbeddedDocumentField(Forward)) 
    n_comments = IntField()
    comments = ListField(EmbeddedDocumentField(Comment))
    
    meta = {
        'indexes': [
            {'fields': ['mid', 'uid']}
        ]
    }
    
class EduInfo(EmbeddedDocument):
    name = StringField()
    date = StringField()
    detail = StringField()
    
class WorkInfo(EmbeddedDocument):
    name = StringField()
    date = StringField()
    location = StringField()
    position = StringField()
    detail = StringField()
    
class UserInfo(EmbeddedDocument):
    nickname = StringField()
    avatar = URLField()
    location = StringField()
    sex = BooleanField()
    birth = StringField()
    blog = URLField()
    site = URLField()
    intro = StringField()
    
    email = EmailField()
    qq = StringField()
    msn = StringField()
    
    n_follows = IntField()
    n_fans = IntField()
    
    edu = ListField(EmbeddedDocumentField(EduInfo))
    work = ListField(EmbeddedDocumentField(WorkInfo))
    tags = ListField(StringField())
    
class Friend(EmbeddedDocument):
    uid = StringField()
    nickname = StringField()
    sex = BooleanField
    
class WeiboUser(Document):
    uid = StringField(required=True)
    last_update = DateTimeField()
    newest_mids = ListField(StringField())
    
    info = EmbeddedDocumentField(UserInfo)
    follows = ListField(EmbeddedDocumentField(Friend))
    fans = ListField(EmbeddedDocumentField(Friend))
    
    meta = {
        'shard_key': shard_key
    }
