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

Created on 2013-6-16

@author: Chine
'''

from urlparse import urlparse

from cola.core.errors import DependencyNotInstalledError

try:
    from bs4 import BeautifulSoup, FeatureNotFound
except ImportError:
    raise DependencyNotInstalledError("BeautifulSoup4")

def beautiful_soup(html, logger=None):
    try:
        return BeautifulSoup(html, 'lxml')
    except FeatureNotFound:
        if logger is not None:
            logger.info('lxml not installed')
        return BeautifulSoup(html)

def host_for_url(url):
    """
    >>> host_for_url('http://base/whatever/fdsh')
    'base'
    >>> host_for_url('invalid')
    """
    host = urlparse(url)[1]
    if not host:
#         raise ValueError("could not extract host from URL: %r" % (url,))
        return None
    return host

def absolute_url(url, base_href):
    """
    >>> absolute_url('foo', 'http://base/whatever/ooo/fdsh')
    'http://base/whatever/ooo/foo'

    >>> absolute_url('foo/bar/', 'http://base')
    'http://base/foo/bar/'

    >>> absolute_url('/foo/bar', 'http://base/whatever/fdskf')
    'http://base/foo/bar'

    >>> absolute_url('\\n/foo/bar', 'http://base/whatever/fdskf')
    'http://base/foo/bar'

    >>> absolute_url('http://localhost/foo', 'http://base/whatever/fdskf')
    'http://localhost/foo'
    """
    url = url.strip()
    proto = urlparse(url)[0]
    if proto:
        return url

    base_url_parts = urlparse(base_href)
    base_server = '://'.join(base_url_parts[:2])
    if url.startswith('/'):
        return base_server + url
    else:
        path = base_url_parts[2]
        if '/' in path:
            path = path.rsplit('/', 1)[0] + '/'
        else:
            path = '/'
        return base_server + path + url

if __name__ == '__main__':
    import doctest
    doctest.testmod()