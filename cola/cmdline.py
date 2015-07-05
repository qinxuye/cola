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

Created on 2015-6-27

@author: chine
'''

import argparse

from cola.commands.job import JobCommand
from cola.commands.master import MasterCommand
from cola.commands.worker import WorkerCommand
from cola.commands.startproject import StartProjectCommand

def execute():
    parser = argparse.ArgumentParser(prog='coca')
    sub_parsers = parser.add_subparsers(help='sub-commands')
    for command_cls in (JobCommand, MasterCommand, WorkerCommand, StartProjectCommand):
        command = command_cls()
        command.add_arguments(sub_parsers)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    execute()