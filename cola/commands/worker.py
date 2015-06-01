#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2015-6-1

@author: chine
'''

from cola.commands import Command
from cola.core.utils import get_ip
from cola.context import Context

class WorkerCommand(Command):
    def add_arguments(self, parser):
        ip = get_ip()
        
        self.worker_parser = parser.add_parser('master', help='worker commands')
        self.worker_parser.add_argument('-m', '--master', metavar='master address', nargs='?', default=ip,
                                        help='master connected to(in the former of `ip:port` or `ip`)')
        self.worker_parser.add_argument('-s', '--start', metavar='worker address', nargs='?', const=ip,
                                        help='local worker connected to(in the former of `ip:port` or `ip`')
        self.worker_parser.set_defaults(func=self.run)
        
    def run(self, args):
        if args.start is not None and args.master is not None:
            ctx = Context(master_addr=args.master, ip=args.start)
            ctx.start_worker()
        else:
            print 'unknown command options'