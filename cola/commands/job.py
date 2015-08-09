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

import os
import tempfile
import pprint
import shutil

from cola.commands import Command
from cola.context import Context
from cola.core.utils import get_ip, import_job_desc
from cola.core.zip import ZipHandler
from cola.core.rpc import FileTransportClient, client_call
from cola.core.logs import get_logger

class JobCommand(Command):
    def __init__(self):
        self.logger = get_logger('cola_job_command')

    def add_arguments(self, parser):
        ip = get_ip()
        
        self.job_parser = parser.add_parser('job', help='job commands')
        self.job_parser.add_argument('-m', '--master', metavar='master address', nargs='?', default=ip,
                                     help='master connected to(in the former of `ip:port` or `ip`)')
        self.job_parser.add_argument('-l', '--list', action='store_true',
                                     help='list all jobs including <id> <name> and <status>' )
        self.job_parser.add_argument('-k', '--kill', metavar='job name', nargs='?',
                                     help='kill job by job name')
        self.job_parser.add_argument('-u', '--upload', metavar='job directory', nargs='?',
                                     help='upload a job directory to the cluster')
        self.job_parser.add_argument('-r', '--run', metavar='job name', nargs='?', const='U',
                                     help='run a job by the job id or with the `upload` command')
        self.job_parser.add_argument('-t', '--status', metavar='job name', nargs='?',
                                     help='show the status of a job, and the counters if it\'s running')
        self.job_parser.add_argument('-p', '--package', metavar='job_name', nargs='?',
                                     help='package the running info of a job including log and errors infos')
        self.job_parser.set_defaults(func=self.run)

    def _get_matched_job_name(self, ctx, job_id):
        jobs = ctx.list_jobs()
        if job_id in jobs:
            matched_jobs = [job_id]
        else:
            matched_jobs = [job for job in jobs if job_id in job]

        if len(matched_jobs) > 1:
            self.logger.error('matched job id is')
            for matched_job in matched_jobs:
                self.logger.info('====> %s' % matched_job)
            self.logger.error('please specify the job id more clearly')
        elif len(matched_jobs) == 0:
            self.logger.error('no job id <%s> exists' % job_id)
        else:
            job_id = matched_jobs[0]
            return job_id

    def run(self, args):
        master_addr = args.master
        ctx = Context(is_client=True, master_addr=master_addr)

        if args.list is True:
            jobs = ctx.list_jobs()
            self.logger.info('list jobs at master: %s' % ctx.master_addr)
            for job_id, info in jobs.iteritems():
                self.logger.info(
                    '====> job id: %s, job description: %s, status: %s' % \
                    (job_id, info['name'], info['status']))
            if len(jobs) == 0:
                self.logger.info('no jobs exist')
        elif args.kill is not None:
            job_id = self._get_matched_job_name(ctx, args.kill)
            if job_id is not None:
                ctx.kill_job(job_id)
                self.logger.info('killed job: %s' % job_id)
        elif args.upload is not None:
            path = os.path.abspath(args.upload)
            if not os.path.exists(path):
                self.logger.error('upload path does not exist')
                return

            job_id = None
            try:
                job_id = import_job_desc(path).uniq_name
            except Exception, e:
                self.logger.exception(e)
                self.logger.error('uploading job description failed')
                return

            new_upload_dir = os.path.join(tempfile.gettempdir(), job_id)
            if os.path.exists(new_upload_dir):
                shutil.rmtree(new_upload_dir)
            shutil.copytree(path, new_upload_dir)

            temp_filename = os.path.join(tempfile.gettempdir(), job_id+'.zip')
            ZipHandler.compress(temp_filename, new_upload_dir, type_filters=('pyc', ))
            try:
                FileTransportClient(ctx.master_addr, temp_filename).send_file()
            finally:
                os.remove(temp_filename)
                shutil.rmtree(new_upload_dir)
            self.logger.info('upload job <id: %s> finished' % job_id)
            
            if args.run == 'U':
                client_call(ctx.master_addr, 'run_job', job_id, True)
                self.logger.info('submit job <id: %s> to the cluster' % job_id)
        elif args.run is not None and args.run != 'U':
            job_id = args.run
            job_id = self._get_matched_job_name(ctx, job_id)
            if job_id is not None:
                client_call(ctx.master_addr, 'run_job', job_id)
                self.logger.info('start to run job: %s' % job_id)
        elif args.status is not None:
            job_id = args.status
            job_id = self._get_matched_job_name(ctx, job_id)
            if job_id is not None:
                jobs = ctx.list_jobs()
                info = jobs[job_id]
                self.logger.info(
                    '====> job id: %s, job description: %s, status: %s' % \
                    (job_id, info['name'], info['status']))
                if info['status'] == 'running':
                    self.logger.info('====> counter:\n' \
                                     + pprint.pformat(ctx.get_job_counter(job_id), width=1))
        elif args.package is not None:
            job_id = args.package
            job_id = self._get_matched_job_name(ctx, job_id)
            if job_id is None:
                return
            master_error_packed_path = ctx.pack_job_error(job_id)
            self.logger.info(
                ('job %s error information files are zipped in the master directory:\n'
                + '====> master addr: %s\n'
                + '====> master zip file location: %s') % 
                (job_id,  ctx.master_addr,  master_error_packed_path))
        else:
            self.logger.error('unknown command options')