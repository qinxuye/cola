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

Created on 2013-6-22

@author: Chine
'''

import socket
import os

from cola.core.rpc import client_call
from cola.core.utils import get_ip, root_dir, import_job
from cola.core.logs import get_logger

from conf import user_config

logger = get_logger(name='sina_stop')

def _recover():
    job_path = os.path.split(os.path.abspath(__file__))[0]
    job = import_job(job_path)

    data_path = os.path.join(root_dir(), 'data')
    root = os.path.join(data_path, 'worker', 'jobs', job.real_name)
    if os.path.exists(root):
        lock_path = os.path.join(root, 'lock')
        if os.path.exists(lock_path):
            os.remove(lock_path)

        def _recover_dir(dir_):
            for f in os.listdir(dir_):
                if f.endswith('.old'):
                    f_path = os.path.join(dir_, f)
                    os.remove(f_path)

            for f in os.listdir(dir_):
                if f == 'lock':
                    lock_f = os.path.join(dir_, f)
                    os.remove(lock_f)

                f_path = os.path.join(dir_, f)
                if os.path.isfile(f_path) and not f.endswith('.old'):
                    os.rename(f_path, f_path+'.old')

        mq_store_dir = os.path.join(root, 'store')
        mq_backup_dir = os.path.join(root, 'backup')
        if os.path.exists(mq_store_dir):
            _recover_dir(mq_store_dir)
        if os.path.exists(mq_backup_dir):
            _recover_dir(mq_backup_dir)


if __name__ == '__main__':
    ip, port = get_ip(), getattr(user_config.job, 'port')
    logger.info('Trying to stop single running worker')
    try:
        client_call('%s:%s' % (ip, port), 'stop')
    except socket.error:
        stop = raw_input("Force to stop? (y or n) ").strip()
        if stop == 'y' or stop == 'yes':
            _recover()
        else:
            print 'ignore'
    logger.info('Successfully stopped single running worker')
