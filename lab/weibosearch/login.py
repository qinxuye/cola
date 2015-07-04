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

class WeiboLogin(object):
    def __init__(self, opener, username, passwd):
        self.opener = opener
        
        self.username = username
        self.passwd = passwd
        
    def login(self):
        br = self.opener.spynner_open('http://weibo.com')
        self.opener.wait_for_selector('div.info_list')
        br.wk_fill('input[name=username]', self.username)
        br.wk_fill('input[name=password]', self.passwd)
        br.click('a.W_btn_g')
        try:
            br.wait_for_content(lambda br: 'WB_feed' in br.html)
            return True
        except:
            return False