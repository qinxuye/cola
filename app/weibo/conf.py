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

import os

from cola.core.config import Config

base = os.path.dirname(os.path.abspath(__file__))
user_conf = os.path.join(base, 'test.yaml')
if not os.path.exists(user_conf):
    user_conf = os.path.join(base, 'weibo.yaml')
user_config = Config(user_conf)

starts = [str(start.uid) for start in user_config.job.starts]

mongo_host = user_config.job.mongo.host
mongo_port = user_config.job.mongo.port
db_name = user_config.job.db

try:
    shard_key = user_config.job.mongo.shard_key
    shard_key = tuple([itm['key'] for itm in shard_key])
except AttributeError:
    shard_key = tuple()

instances = user_config.job.instances

fetch_forward = user_config.job.fetch.forward
fetch_comment = user_config.job.fetch.comment
fetch_like = user_config.job.fetch.like