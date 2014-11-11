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
import re
from urllib2 import URLError
from datetime import datetime, timedelta
from threading import Lock

from cola.core.parsers import Parser
from cola.core.utils import urldecode, beautiful_soup
from cola.core.errors import DependencyNotInstalledError
from cola.core.logs import get_logger

from login import WeiboLoginFailure
from bundle import WeiboUserBundle
from storage import DoesNotExist, Q, WeiboUser, Friend,\
                    MicroBlog, Geo, UserInfo, WorkInfo, EduInfo,\
                    Comment, Forward, Like, ValidationError
from conf import fetch_forward, fetch_comment, fetch_like

try:
    from dateutil.parser import parse
except ImportError:
    raise DependencyNotInstalledError('python-dateutil')

TIMEOUT = 30.0

class WeiboParser(Parser):
    def __init__(self, opener=None, url=None, bundle=None, **kwargs):
        super(WeiboParser, self).__init__(opener=opener, url=url, **kwargs)
        self.bundle = bundle
        self.uid = bundle.label
        self.opener.set_default_timeout(TIMEOUT)
        if not hasattr(self, 'logger') or self.logger is None:
            self.logger = get_logger(name='weibo_parser')
    
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
        if self.bundle.weibo_user is not None:
            return self.bundle.weibo_user
        
        try:
            self.bundle.weibo_user = getattr(WeiboUser, 'objects').get(uid=self.uid)
        except DoesNotExist:
            self.bundle.weibo_user = WeiboUser(uid=self.uid)
            self.bundle.weibo_user.save()
        return self.bundle.weibo_user
    
    def _error(self, url, e):
        if self.bundle.last_error_page == url:
            self.bundle.last_error_page_times += 1
        else:
            self.bundle.last_error_page = url
            self.bundle.last_error_page_times = 0
            
        if self.bundle.last_error_page_times >= 15:
            raise e
        return [url, ], []

class MicroBlogParser(WeiboParser):
    def parse(self, url=None):
        if self.bundle.exists == False:
            return [], []
        
        url = url or self.url
        params = urldecode(url)
        br = self.opener.browse_open(url)
        self.logger.debug('load %s finish' % url)
        
        if not self.check(url, br):
            return [], []
            
        weibo_user = self.get_weibo_user()
        
        params['_t'] = 0
        params['__rnd'] = str(int(time.time() * 1000))
        page = int(params.get('page', 1))
        pre_page = int(params.get('pre_page', 0))
        count = 15
        if 'pagebar' not in params:
            params['pagebar'] = '0'
            pre_page += 1
        elif params['pagebar'] == '0':
            params['pagebar'] = '1'
        elif params['pagebar'] == '1':
            del params['pagebar']
            pre_page = page
            page += 1
            count = 50
        params['count'] = count
        params['page'] = page
        params['pre_page'] = pre_page
        
        data = json.loads(br.response().read())['data']
        soup = beautiful_soup(data)
        finished = False
        
        divs = soup.find_all('div', attrs={'class': 'WB_feed_type'},  mid=True)
        max_id = None
        next_urls = []
        for div in divs:
            mid = div['mid']
            if len(mid) == 0:
                continue
            max_id = mid
            
            if 'end_id' not in params:
                params['end_id'] = mid
            if mid in weibo_user.newest_mids:
                finished = True
                break
            if len(self.bundle.newest_mids) < 3:
                self.bundle.newest_mids.append(mid)
            
            try:
                mblog = getattr(MicroBlog, 'objects').get(Q(mid=mid)&Q(uid=self.uid))
            except DoesNotExist:
                mblog = MicroBlog(mid=mid, uid=self.uid)
            content_div = div.find('div', attrs={
                'class': 'WB_text', 
                'node-type': 'feed_list_content'
            })
            for img in content_div.find_all("img", attrs={'type': 'face'}):
                img.replace_with(img['title']);
            mblog.content = content_div.text
            is_forward = div.get('isforward') == '1'
            if is_forward:
                mblog.omid = div['omid']
                name_a = div.find('a', attrs={
                    'class': 'WB_name', 
                    'node-type': 'feed_list_originNick'
                })
                text_a = div.find('div', attrs={
                    'class': 'WB_text',
                    'node-type': 'feed_list_reason'
                })
                if name_a is not None and text_a is not None:
                    mblog.forward = '%s: %s' % (
                        name_a.text,
                        text_a.text
                    )
            mblog.created = parse(div.select('a.S_link2.WB_time')[0]['title'])
            
            if self.bundle.last_update is None or mblog.created > self.bundle.last_update:
                self.bundle.last_update = mblog.created
            if weibo_user.last_update is not None and \
                mblog.created <= weibo_user.last_update:
                finished = True
                break

            func_div = div.find_all('div', 'WB_func')[-1]
            action_type_re = lambda t: re.compile("^(feed_list|fl)_%s$" % t)
            
            likes = func_div.find('a', attrs={'action-type': action_type_re("like")}).text
            likes = likes.strip('(').strip(')')
            likes = 0 if len(likes) == 0 else int(likes)
            mblog.n_likes = likes
            forwards = func_div.find('a', attrs={'action-type': action_type_re("forward")}).text
            if '(' not in forwards:
                mblog.n_forwards = 0
            else:
                mblog.n_forwards = int(forwards.strip().split('(', 1)[1].strip(')'))
            comments = func_div.find('a', attrs={'action-type': action_type_re('comment')}).text
            if '(' not in comments:
                mblog.n_comments = 0
            else:
                mblog.n_comments = int(comments.strip().split('(', 1)[1].strip(')'))
                
            # fetch geo info
            map_info = div.find("div", attrs={'class': 'map_data'})
            if map_info is not None:
                geo = Geo()
                geo.location = map_info.text.split('-')[0].strip()
                geo_info = urldecode("?"+map_info.find('a')['action-data'])['geo']
                geo.longtitude, geo.latitude = tuple([float(itm) for itm in geo_info.split(',', 1)])
                mblog.geo = geo
            
            # fetch forwards and comments
            if fetch_forward or fetch_comment or fetch_like:
                query = {'id': mid, '_t': 0, '__rnd': int(time.time()*1000)}
                query_str = urllib.urlencode(query)
                if fetch_forward and mblog.n_forwards > 0:
                    forward_url = 'http://weibo.com/aj/mblog/info/big?%s' % query_str
                    next_urls.append(forward_url)
                if fetch_comment and mblog.n_comments > 0:
                    comment_url = 'http://weibo.com/aj/comment/big?%s' % query_str
                    next_urls.append(comment_url)
                if fetch_like and mblog.n_likes > 0:
                    query = {'mid': mid, '_t': 0, '__rnd': int(time.time()*1000)}
                    query_str = urllib.urlencode(query)
                    like_url = 'http://weibo.com/aj/like/big?%s' % query_str
                    next_urls.append(like_url)
            
            mblog.save()
        
        if 'pagebar' in params:
            params['max_id'] = max_id
        else:
            del params['max_id']
        self.logger.debug('parse %s finish' % url)
                
        # if not has next page
        if len(divs) == 0 or finished:
            weibo_user = self.get_weibo_user()
            for mid in self.bundle.newest_mids:
                if mid not in weibo_user.newest_mids:
                    weibo_user.newest_mids.append(mid)
            while len(weibo_user.newest_mids) > 3:
                weibo_user.newest_mids.pop()
            weibo_user.last_update = self.bundle.last_update
            weibo_user.save()
            return [], []
        
        next_urls.append('%s?%s'%(url.split('?')[0], urllib.urlencode(params)))
        return next_urls, []
    
class ForwardCommentLikeParser(WeiboParser):
    strptime_lock = Lock()
    
    def _strptime(self, string, format_):
        self.strptime_lock.acquire()
        try:
            return datetime.strptime(string, format_)
        finally:
            self.strptime_lock.release()
        
    def parse_datetime(self, dt_str):
        dt = None
        if u'秒' in dt_str:
            sec = int(dt_str.split(u'秒', 1)[0].strip())
            dt = datetime.now() - timedelta(seconds=sec)
        elif u'分钟' in dt_str:
            sec = int(dt_str.split(u'分钟', 1)[0].strip()) * 60
            dt = datetime.now() - timedelta(seconds=sec)
        elif u'今天' in dt_str:
            dt_str = dt_str.replace(u'今天', datetime.now().strftime('%Y-%m-%d'))
            dt = self._strptime(dt_str, '%Y-%m-%d %H:%M')
        elif u'月' in dt_str and u'日' in dt_str:
            this_year = datetime.now().year
            date_str = '%s %s' % (this_year, dt_str)
            if isinstance(date_str, unicode):
                date_str = date_str.encode('utf-8')
            dt = self._strptime(date_str, '%Y %m月%d日 %H:%M')
        else:
            dt = parse(dt_str)
        return dt
    
    def parse(self, url=None):
        if self.bundle.exists == False:
            return [], []
        
        url = url or self.url
        br = None
        jsn = None
        try:
            br = self.opener.browse_open(url)
            self.logger.debug('load %s finish' % url)
            jsn = json.loads(br.response().read())
        except (ValueError, URLError) as e:
            return self._error(url, e)
        
        soup = beautiful_soup(jsn['data']['html'])
        current_page = jsn['data']['page']['pagenum']
        n_pages = jsn['data']['page']['totalpage']
        
        if not self.check(url, br):
            return [], []
        
        decodes = urldecode(url)
        mid = decodes.get('id', decodes.get('mid'))
        
        mblog = self.bundle.current_mblog
        if mblog is None or mblog.mid != mid:
            try:
                mblog = getattr(MicroBlog, 'objects').get(Q(mid=mid)&Q(uid=self.uid))
            except DoesNotExist:
                mblog = MicroBlog(mid=mid, uid=self.uid)
                mblog.save()
        
        def set_instance(instance, dl):
            instance.avatar = dl.find('dt').find('img')['src']
            date = dl.find('dd').find(attrs={'class': 'S_txt2'}).text
            date = date.strip().strip('(').strip(')')
            instance.created = self.parse_datetime(date)
            for div in dl.find_all('div'): div.extract()
            for span in dl.find_all('span'): span.extract()
            instance.content = dl.text.strip()
        
        if url.startswith('http://weibo.com/aj/comment'):
            dls = soup.find_all('dl', mid=True)
            for dl in dls:
                uid = dl.find('a', usercard=True)['usercard'].split("id=", 1)[1]
                comment = Comment(uid=uid)
                set_instance(comment, dl)
                
                mblog.comments.append(comment)
        elif url.startswith('http://weibo.com/aj/mblog/info'):
            dls = soup.find_all('dl', mid=True)
            for dl in dls:
                forward_again_a = dl.find('a', attrs={'action-type': re.compile("^(feed_list|fl)_forward$")})
                uid = urldecode('?%s' % forward_again_a['action-data'])['uid']
                forward = Forward(uid=uid, mid=dl['mid'])
                set_instance(forward, dl)
                
                mblog.forwards.append(forward)
        elif url.startswith('http://weibo.com/aj/like'):
            lis = soup.find_all('li', uid=True)
            for li in lis:
                like = Like(uid=li['uid'])
                like.avatar = li.find('img')['src']
                
                mblog.likes.append(like)
        
        try:
            mblog.save()
            self.logger.debug('parse %s finish' % url)
        except ValidationError, e:
            return self._error(url, e)
        
        if current_page >= n_pages:
            return [], []
        
        params = urldecode(url)
        new_params = urldecode('?page=%s'%(current_page+1))
        params.update(new_params)
        params['__rnd'] = int(time.time()*1000)
        next_page = '%s?%s' % (url.split('?')[0] , urllib.urlencode(params))
        return [next_page, ], []
    
class UserInfoParser(WeiboParser):
    def parse(self, url=None):
        if self.bundle.exists == False:
            return [], []
        
        url = url or self.url
        br = self.opener.browse_open(url)
        self.logger.debug('load %s finish' % url)
        soup = beautiful_soup(br.response().read())
        
        if not self.check(url, br):
            return [], []
        
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
            if text.startswith('FM.view'):
                text = text.strip().replace(';', '').replace('FM.view(', '')[:-1]
                data = json.loads(text)
                domid = data['domid']
                if domid.startswith('Pl_Official_LeftInfo__'):
                    info_soup = beautiful_soup(data['html'])
                    info_div = info_soup.find('div', attrs={'class': 'profile_pinfo'})
                    for block_div in info_div.find_all('div', attrs={'class': 'infoblock'}):
                        block_title = block_div.find('form').text.strip()
                        if block_title == u'基本信息':
                            profile_div = block_div
                        elif block_title == u'工作信息':
                            career_div = block_div
                        elif block_title == u'教育信息':
                            edu_div = block_div
                        elif block_title == u'标签信息':
                            tags_div = block_div
                elif domid == 'Pl_Official_Header__1':
                    header_soup = beautiful_soup(data['html'])
                    weibo_user.info.avatar = header_soup.find('div', attrs={'class': 'pf_head_pic'})\
                                                .find('img')['src']
                    weibo_user.info.n_follows = int(header_soup.find('ul', attrs={'class': 'user_atten'})\
                                                    .find('strong', attrs={'node-type': 'follow'}).text)
                    weibo_user.info.n_fans = int(header_soup.find('ul', attrs={'class': 'user_atten'})\
                                                 .find('strong', attrs={'node-type': 'fans'}).text)
                                                    
            elif 'STK' in text:
                text = text.replace('STK && STK.pageletM && STK.pageletM.view(', '')[:-1]
                data = json.loads(text)
                pid = data['pid']
                if pid == 'pl_profile_infoBase':
                    profile_div = beautiful_soup(data['html'])
                elif pid == 'pl_profile_infoCareer':
                    career_div = beautiful_soup(data['html'])
                elif pid == 'pl_profile_infoEdu':
                    edu_div = beautiful_soup(data['html'])
                elif pid == 'pl_profile_infoTag':
                    tags_div = beautiful_soup(data['html'])
                elif pid == 'pl_profile_photo':
                    soup = beautiful_soup(data['html'])
                    weibo_user.info.avatar = soup.find('img')['src']
        
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
        if profile_div is not None:
            for div in profile_div.find_all(attrs={'class': 'pf_item'}):
                k = div.find(attrs={'class': 'label'}).text.strip()
                v = div.find(attrs={'class': 'con'}).text.strip()
                if k in profile_map:
                    if k == u'个性域名' and '|' in v:
                        v = v.split('|')[1].strip()
                    func = (lambda s: s) \
                            if 'func' not in profile_map[k] \
                            else profile_map[k]['func']
                    v = func(v)
                    setattr(weibo_user.info, profile_map[k]['field'], v)
                
        weibo_user.info.work = []
        if career_div is not None:
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
                            work_info.location = text.split(u'：', 1)[1]
                        elif text.startswith(u'职位：'):
                            work_info.position = text.split(u'：', 1)[1]
                        else:
                            work_info.detail = text
                weibo_user.info.work.append(work_info)
            
        weibo_user.info.edu = []
        if edu_div is not None:
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
        if tags_div is not None:
            for div in tags_div.find_all(attrs={'class': 'con'}):
                for a in div.find_all('a'):
                    weibo_user.info.tags.append(a.text)
                
        weibo_user.save()
        self.logger.debug('parse %s finish' % url)
        return [], []
    
class UserFriendParser(WeiboParser):
    def parse(self, url=None):
        if self.bundle.exists == False:
            return [], []
        
        url = url or self.url
        
        br, soup = None, None
        try:
            br = self.opener.browse_open(url)
#             self.logger.debug('load %s finish' % url)
            soup = beautiful_soup(br.response().read())
        except Exception, e:
            return self._error(url, e)
        
        if not self.check(url, br):
            return [], []
        
        weibo_user = self.get_weibo_user()
        
        html = None
        decodes = urldecode(url)
        is_follow = True
        is_new_mode = False
        for script in soup.find_all('script'):
            text = script.text
            if text.startswith('FM.view'):
                text = text.strip().replace(';', '').replace('FM.view(', '')[:-1]
                data = None
                try:
                    data = json.loads(text)
                except ValueError, e:
                    return self._error(url, e)
                domid = data['domid']
                if domid.startswith('Pl_Official_LeftHisRelation__') or \
                    domid.startswith('Pl_Official_HisRelation__'):
                    html = beautiful_soup(data['html'])
                if 'relate' in decodes and decodes['relate'] == 'fans':
                    is_follow = False
                is_new_mode = True
            elif 'STK' in text:
                text = text.replace('STK && STK.pageletM && STK.pageletM.view(', '')[:-1]
                data = json.loads(text)
                if data['pid'] == 'pl_relation_hisFollow' or \
                    data['pid'] == 'pl_relation_hisFans':
                    html = beautiful_soup(data['html'])
                if data['pid'] == 'pl_relation_hisFans':
                    is_follow = False    
        
        bundles = []
        ul = None
        try:
            ul = html.find(attrs={'class': 'cnfList', 'node-type': 'userListBox'})
            if ul is None:
                ul = html.find(attrs={'class': 'follow_list', 'node-type': 'userListBox'})
        except AttributeError, e:
            if br.geturl().startswith('http://e.weibo.com'):
                return [], []
            return self._error(url, e)
        
        if ul is None:
            urls = []
            if is_follow is True:
                if is_new_mode:
                    urls.append('http://weibo.com/%s/follow?relate=fans' % self.uid)
                else:
                    urls.append('http://weibo.com/%s/fans' % self.uid)
            return urls, bundles
        
        current_page = decodes.get('page', 1)
        if current_page == 1:
            if is_follow:
                weibo_user.follows = []
            else:
                weibo_user.fans = []
        for cls in ('S_line1', 'S_line2'):
            for li in ul.find_all(attrs={'class': cls, 'action-type': 'itemClick'}):
                data = dict([l.split('=') for l in li['action-data'].split('&')])
                
                friend = Friend()
                friend.uid = data['uid']
                friend.nickname = data['fnick']
                friend.sex = True if data['sex'] == u'm' else False
                
                bundles.append(WeiboUserBundle(str(friend.uid)))
                if is_follow:
                    weibo_user.follows.append(friend)
                else:
                    weibo_user.fans.append(friend)
                
        weibo_user.save()
#         self.logger.debug('parse %s finish' % url)
        
        urls = []
        pages = html.find('div', attrs={'class': 'W_pages', 'node-type': 'pageList'})
        if pages is None:
            pages = html.find('div', attrs={'class': 'WB_cardpage', 'node-type': 'pageList'})
        if pages is not None:
            a = pages.find_all('a')
            if len(a) > 0:
                next_ = a[-1]
                if next_['class'] == ['W_btn_c'] or 'next' in next_['class']:
                    decodes['page'] = int(decodes.get('page', 1)) + 1
                    query_str = urllib.urlencode(decodes)
                    url = '%s?%s' % (url.split('?')[0], query_str)
                    urls.append(url)
                    
                    return urls, bundles
        
        if is_follow is True:
            if is_new_mode:
                urls.append('http://weibo.com/%s/follow?relate=fans' % self.uid)
            else:
                urls.append('http://weibo.com/%s/fans' % self.uid)
        
        return urls, bundles
