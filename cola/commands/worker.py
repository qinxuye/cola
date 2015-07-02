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

Created on 2015-6-1

@author: chine
'''

from cola.commands import Command
from cola.core.utils import get_ip
from cola.context import Context
from cola.core.logs import get_logger

class WorkerCommand(Command):
    def __init__(self):
        self.logger = get_logger('cola_worker_command')

    def add_arguments(self, parser):
        ip = get_ip()
        
        self.worker_parser = parser.add_parser('worker', help='worker commands')
        self.worker_parser.add_argument('-m', '--master', metavar='master address', nargs='?', default=ip,
                                        help='master connected to(in the former of `ip:port` or `ip`)')
        self.worker_parser.add_argument('-s', '--start', metavar='worker address', nargs='?', const=ip,
                                        help='local worker connected to(in the former of `ip:port` or `ip`')
        self.worker_parser.set_defaults(func=self.run)
        
    def run(self, args):
        if args.start is not None and args.master is not None:
            ctx = Context(master_addr=args.master, ip=args.start)
            ctx.start_worker()
            self.logger.info('start worker at: %s' % ctx.worker_addr)
        else:
            self.logger.error('unknown command options')