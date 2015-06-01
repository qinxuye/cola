#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2015-6-1

@author: chine
'''

from cola.commands import Command
from cola.core.utils import get_ip

class JobCommand(Command):
    def add_arguments(self, parser):
        ip = get_ip()
        
        self.job_parser = parser.add_parser('master', help='job commands')
        self.job_parser.add_argument('-m', '--master', metavar='master address', nargs='?', default=ip,
                                     help='master connected to(in the former of `ip:port` or `ip`)')
        