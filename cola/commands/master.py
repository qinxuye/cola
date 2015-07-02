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

Created on 2015-2-17

@author: chine
'''

from cola.commands import Command
from cola.context import Context
from cola.core.utils import get_ip
from cola.core.logs import get_logger

class MasterCommand(Command):
    def __init__(self):
        self.logger = get_logger('cola_master_command')

    def add_arguments(self, parser):
        ip = get_ip()
        
        self.master_parser = parser.add_parser('master', help='master commands')
        self.master_parser.add_argument('-s', '--start', metavar='start master', nargs='?', const=ip,
                                        help='master address(in the former of `ip:port` or `ip`)')
        self.master_parser.add_argument('-k', '--kill', metavar='kill master', nargs='?', const=ip,
                                        help='master to kill(in the former of `ip:port` or `ip`)')
        self.master_parser.add_argument('-l', '--list', metavar='list workers', nargs='?', const=ip,
                                        help='list workers(in the former of `ip:port` or `ip`)')
        self.master_parser.set_defaults(func=self.run)
    
    def run(self, args):
        if args.start is not None:
            ctx = Context(is_master=True, master_addr=args.start)
            ctx.start_master()
            self.logger.info('start master at: %s' % ctx.master_addr)
        elif args.kill is not None:
            ctx = Context(is_client=True, master_addr=args.kill)
            ctx.kill_master()
            self.logger.info('kill master at: %s' % ctx.master_addr)
        elif args.list is not None:
            ctx = Context(is_client=True, master_addr=args.list)
            self.logger.info('list workers at master: %s' % ctx.master_addr)
            for worker, status in ctx.list_workers():
                self.logger.info('====> worker: %s, status: %s' % (worker, status))
        else:
            self.logger.error('unknown command options')