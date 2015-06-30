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

Created on 2015-6-7

@author: chine
'''

import os

from cola.commands import Command
from cola.context import Context

class StartProjectCommand(Command):
    def add_arguments(self, parser):
        self.start_project_parser = parser.add_parser('startproject', help='startproject command')
        self.start_project_parser.add_argument('project', metavar='project name', nargs=1, 
                                               help='project name to start')
        self.start_project_parser.set_defaults(func=self.run)
        
    def _replace_variable(self, content, kv):
        for k, v in kv.iteritems():
            content = content.replace('{{ %s }}'%k, v)
        return content
        
    def run(self, args):
        ctx = Context(is_client=True)
        
        project_name = args.project        
        if ' ' in project_name:
            self.logger.error('project name cannot contain whitespace')
        
        current_dir = os.getcwd()
        project_dir = os.path.join(current_dir, project_name)
        if not os.path.exists(project_dir):
            os.makedirs(project_dir)
        
        for filename, src_filename in (('%s.yaml'%project_name, 'project.yaml.tmpl'), 
                                       ('__init__.py', 'project.py.tmpl')):
            full_filename = os.path.join(project_dir, filename)
            full_temp_filename = os.path.join(ctx.get_cola_dir(), 'templates', src_filename)
            with open(full_temp_filename) as temp_fp:
                with open(full_filename, 'w') as fp:
                    content = self._replace_variable(temp_fp.read(), {'name': project_name})
                    fp.write(content)
                    
        self.logger.info('create project: %s' % project_name)