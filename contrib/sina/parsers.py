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

Created on 2013-6-8

@author: Chine
'''

import time
import json
import urllib

from cola.core.parsers import Parser
from cola.core.utils import urldecode
from cola.core.errors import DependencyNotInstalledError

from login import WeiboLoginFailure
from storage import DoesNotExist, WeiboUser, \
                    MicroBlog, UserInfo, WorkInfo, EduInfo

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise DependencyNotInstalledError('BeautifulSoup4')

try:
    from dateutil.parser import parse
except ImportError:
    raise DependencyNotInstalledError('python-dateutil')

class WeiboParser(Parser):
    def __init__(self, opener=None, url=None, bundle=None, **kwargs):
        super(WeiboParser, self).__init__(opener=opener, url=url, **kwargs)
        self.bundle = bundle
        self.uid = bundle.label
    
    def _check_url(self, dest_url, src_url):
        return dest_url.split('?')[0] == src_url.split('?')[0]
    
    def check(self, url, br):
        dest_url = br.geturl()
        if not self._check_url(dest_url, url):
            if dest_url.startswith('http://weibo.com/login.php'):
                raise WeiboLoginFailure('Weibo not login or login expired')
            if dest_url.startswith('http://weibo.com/sorry?usernotexists'):
                self.bundle.exists = False
                return False
        return True
    
    def get_weibo_user(self):
        weibo_user = None
        try:
            weibo_user = getattr(WeiboUser, 'objects').get(uid=self.uid)
        except DoesNotExist:
            weibo_user = WeiboUser(uid=self.uid)
            weibo_user.save()
        return weibo_user

class MicroBlogParser(WeiboParser):
    def parse(self, url=None):
        if self.bundle.exists == False:
            return
        
        url = url or self.url
        params = urldecode(url)
        br = self.opener.browse_open(url)
        
        if not self.check(url, br):
            return
            
        weibo_user = self.get_weibo_user()
        statuses = weibo_user.statuses
        
        params['_t'] = 0
        params['__rnd'] = str(int(time.time() * 1000))
        page = params.get('page', 1)
        pre_page = params.get('pre_page', 1)
        if 'pagebar' not in params:
            params['pagebar'] = '0'
        elif params['pagebar'] == '0':
            params['pagebar'] = '1'
        elif params['pagebar'] == '1':
            del params['pagebar']
            pre_page = page
            page += 1
        count = 15
        params['count'] = count
        params['page'] = page
        params['pre_page'] = pre_page
        
        data = json.loads(br.response().read())['data']
        soup = BeautifulSoup(data)
        
        divs = soup.find_all('div', attrs={'class': 'WB_feed_type'},  mid=True)
        max_id = None
        for div in divs:
            mid = div['mid']
            if len(mid) == 0:
                continue
            max_id = mid
            
            if 'end_id' not in params:
                params['end_id'] = mid
            if weibo_user.newest_mid is not None and \
                weibo_user.newest_mid == mid:
                break
            
            mblog = MicroBlog(mid=mid)
            mblog.content = div.find('div', attrs={
                'class': 'WB_text', 
                'node-type': 'feed_list_content'
            }).text
            is_forward = div.get('isforward') == '1'
            if is_forward:
                mblog.forward = '%s: %s' % (
                    div.find('a', attrs={
                        'class': 'WB_name', 
                        'node-type': 'feed_list_originNick'
                    }).text,
                    div.find('div', attrs={
                        'class': 'WB_text',
                        'node-type': 'feed_list_reason'
                    }).text
                )
            mblog.created = parse(div.select('a.S_link2.WB_time')[0]['title'])
            likes = div.find('a', attrs={'action-type': 'feed_list_like'}).text
            likes = likes.strip('(').strip(')')
            likes = 0 if len(likes) == 0 else int(likes)
            mblog.likes = likes
            forwards = div.find('a', attrs={'action-type': 'feed_list_forward'}).text
            if '(' not in forwards:
                mblog.forwards = 0
            else:
                mblog.forwards = int(forwards.strip().split('(', 1)[1].strip(')'))
            comments = div.find('a', attrs={'action-type': 'feed_list_comment'}).text
            if '(' not in comments:
                mblog.comments = 0
            else:
                mblog.comments = int(comments.strip().split('(', 1)[1].strip(')'))
            
            statuses.append(mblog)
                
        params['max_id'] = max_id
                
        # if not has next page
        if len(divs) < count:
            weibo_user.newest_mid = params['end_id']
            weibo_user.save()
            return [], []
        
        weibo_user.save()
        return ['%s?%s'%(url.split('?')[0], urllib.urlencode(params)), ], []
    
class UserInfoParser(WeiboParser):
    def parse(self, url=None):
        if self.bundle.exists == False:
            return
        
        url = url or self.url
        br = self.opener.browse_open(url)
        soup = BeautifulSoup(br.response().read())
        
        if not self.check(url, br):
            return
        
        weibo_user = self.get_weibo_user()
        info = weibo_user.info
        if info is None:
            weibo_user.info = UserInfo()
            
        profile_div = None
        career_div = None
        edu_div = None
        tags_div = None
        for script in soup.find_all('script'):
            text = script.text
            if 'STK' in text:
                text = text.replace('STK && STK.pageletM && STK.pageletM.view(', '')[:-1]
                data = json.loads(text)
                pid = data['pid']
                if pid == 'pl_profile_infoBase':
                    profile_div = BeautifulSoup(data['html'])
                elif pid == 'pl_profile_infoCareer':
                    career_div = BeautifulSoup(data['html'])
                elif pid == 'pl_profile_infoEdu':
                    edu_div = BeautifulSoup(data['html'])
                elif pid == 'pl_profile_infoTag':
                    tags_div = BeautifulSoup(data['html'])
        
        profile_map = {
            u'昵称': {'field': 'nickname'},
            u'所在地': {'field': 'location'},
            u'性别': {'field': 'sex', 
                    'func': lambda s: True if s == u'男' else False},
            u'生日': {'field': 'birth'},
            u'博客': {'field': 'blog'},
            u'个性域名': {'field': 'site'},
            u'简介': {'field': 'intro'},
            u'邮箱': {'field': 'email'},
            u'QQ': {'field': 'qq'},
            u'MSN': {'field': 'msn'}
        }
        for div in profile_div.find_all(attrs={'class': 'pf_item'}):
            k = div.find(attrs={'class': 'label'}).text.strip()
            v = div.find(attrs={'class': 'con'}).text.strip()
            if k in profile_map:
                func = (lambda s: s) \
                        if 'func' not in profile_map[k] \
                        else profile_map[k]['func']
                v = func(v)
                setattr(weibo_user.info, profile_map[k]['field'], v)
                
        weibo_user.info.work = []
        for div in career_div.find_all(attrs={'class': 'con'}):
            work_info = WorkInfo()
            ps = div.find_all('p')
            for p in ps:
                a = p.find('a')
                if a is not None:
                    work_info.name = a.text
                    text = p.text
                    if '(' in text:
                        work_info.date = text.strip().split('(')[1].strip(')')
                else:
                    text = p.text
                    if text.startswith(u'地区：'):
                        work_info.location = text.split('：', 1)[1]
                    elif text.startswith(u'职位：'):
                        work_info.position = text.split('：', 1)[1]
                    else:
                        work_info.detail = text
            weibo_user.info.work.append(work_info)
            
        weibo_user.info.edu = []
        for div in edu_div.find_all(attrs={'class': 'con'}):
            edu_info = EduInfo()
            ps = div.find_all('p')
            for p in ps:
                a = p.find('a')
                text = p.text
                if a is not None:
                    edu_info.name = a.text
                    if '(' in text:
                        edu_info.date = text.strip().split('(')[1].strip(')')
                else:
                    edu_info.detail = text
            weibo_user.info.edu.append(edu_info)
                    
        weibo_user.info.tags = []
        for div in tags_div.find_all(attrs={'class': 'con'}):
            for a in div.find_all('a'):
                weibo_user.info.tags.append(a.text)
                
        weibo_user.save()
        return [], []