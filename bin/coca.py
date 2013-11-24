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

Created on 2013-6-13

@author: Chine
'''

import argparse
import socket
import threading
import os
import tempfile
import shutil

from cola.core.logs import get_logger, LogRecordSocketReceiver
from cola.core.rpc import client_call, FileTransportClient, ColaRPCServer
from cola.core.utils import import_job, get_ip
from cola.core.zip import ZipHandler
from cola.core.config import main_conf

logger = get_logger(name='coca')
parser = argparse.ArgumentParser('Coca')
registered_func = {}

def _client_call(*args):
    try:
        return client_call(*args)
    except socket.error:
        logger.error('Cannot connect to cola master')

def register(func):
    func_name = func.__name__
    name = '-%s' % func_name.replace('_', '-').strip('-')
    help_ = func.__doc__.strip()
    
    registered_func[func_name] = func
    parser.add_argument(name, nargs='*', dest=func_name,
                        default=argparse.SUPPRESS, help=help_)
    
    def inner(master, *args, **kwargs):
        return func(master, *args, **kwargs)
    return inner

log_server = None
log_server_port = 9120
client = '%s:%s' % (get_ip(), log_server_port)
def start_log_server():
    global log_server
    global log_server_port
    
    if log_server is not None:
        return
    log_server = LogRecordSocketReceiver(logger=logger, host=get_ip(), 
                                         port=log_server_port)
    threading.Thread(target=log_server.serve_forever).start()
    
def stop_log_server():
    global log_server
    
    if log_server is None:
        return
    
    log_server.shutdown()
    log_server = None
    
rpc_server = None
rpc_server_thread = None
def start_rpc_server():
    global rpc_server
    global rpc_server_thread
    
    if rpc_server is not None and \
        rpc_server_thread is not None:
        return rpc_server_thread
    
    rpc_server = ColaRPCServer((get_ip(), main_conf.client.port))
    rpc_server.register_function(stop)
    
    thd = threading.Thread(target=rpc_server.serve_forever)
    thd.setDaemon(True)
    thd.start()
    rpc_server_thread = thd
    return rpc_server_thread

def stop_rpc_server():
    global rpc_server
    global rpc_server_thread
    
    if rpc_server is None:
        return
    
    rpc_server.shutdown()
    rpc_server = None
    rpc_server_thread = None
    
def stop():
    stop_log_server()
    stop_rpc_server()

@register
def stopAll(master):
    '''
    stop cola cluster
    '''
    
    logger.info('Stopping cola cluster.')
    _client_call(master, 'stop')
    logger.info('Cola cluster is shutting down, '
                'and it will take a few seconds to complete.')
    
@register
def runLocalJob(master, job_path):
    '''
    push local job to cola cluster and run
    '''
    
    if not os.path.exists(job_path):
        logger.error('Job path not exists!')
        return
    
    try:
        import_job(job_path)
    except (ImportError, AttributeError):
        logger.error('Job path is illegal!')
        return
    
    start_log_server()
    thread = start_rpc_server()
        
    logger.info('Pushing job to cola cluster...')
    dir_ = tempfile.mkdtemp()
    try:
        zip_filename = os.path.split(job_path)[1].replace(' ', '_') + '.zip'
        zip_file = os.path.join(dir_, zip_filename)
        
        ZipHandler.compress(zip_file, job_path, type_filters=("pyc", ))
        FileTransportClient(master, zip_file).send_file()
        
        logger.info('Push finished.')
    finally:
        shutil.rmtree(dir_)
    
    logger.info('Start to run job.')    
    _client_call(master, 'start_job', zip_filename, True, client)
    thread.join()
    
@register
def showRemoteJobs(master):
    '''
    show the jobs that exists in the cola server
    '''
    
    logger.info('Quering the cola cluster...')
    
    print 'Available jobs: '
    for dir_ in _client_call(master, 'list_job_dirs'):
        print dir_
    
@register
def runRemoteJob(master, job_dir_name):
    '''
    run the job that exists in the cola server
    '''
    
    logger.info('Checking if job dir name exists...')
    if job_dir_name not in _client_call(master, 'list_job_dirs'):
        logger.error('Remote job dir not exists!')
    else:
        logger.info('Start to run job.')
        
        start_log_server()
        thread = start_rpc_server()
        
        _client_call(master, 'start_job', job_dir_name, False, client)
        thread.join()
        
@register
def showRunningJobsNames(master):
    '''
    show the running jobs' names
    '''
    
    logger.info('Querying the cola cluster...')
    
    print 'Running jobs\' names: '
    for job_name in _client_call(master, 'list_jobs'):
        print job_name
        
@register
def stopRunningJobByName(master, job_name):
    '''
    stop running job by its name
    '''
    
    if job_name not in _client_call(master, 'list_jobs'):
        logger.error('The job with name(%s) not running in cola cluster' % job_name)
        logger.info('Please run command `python coca.py --showRunningJobsNames` to check job names.')
    else:
        logger.info('Trying to stop job with name(%s).' % job_name)
        
        _client_call(master, 'stop_job', job_name)
        
        logger.info('Job with name(%s) is shutting down, '
                    'and it will take a few seconds to complete.')
        
@register
def showVisitedPages(master):
    '''
    show all visited pages' size
    '''
    
    logger.info('Querying the cola cluster...')
    
    print 'All vistied page size\' size: %s' % _client_call(master, 'pages')
        
if __name__ == "__main__":
    parser.add_argument('-m', '--master', metavar='master watcher', nargs='?',
                        default=None, const=None,
                        help='master connected to(in the former of `ip:port` or `ip`)')
    args = parser.parse_args()
    
    master = args.master
    if master is None:
        connect_to_localhost = raw_input("Connect to localhost? (yes or no) ")
        conn = connect_to_localhost.lower().strip()
        if conn == 'yes' or conn == 'y':
            master = '%s:%s' % (get_ip(), main_conf.master.port)
        elif conn == 'no' or conn == 'n':
            master = raw_input("Please input the master(form: \"ip:port\" or \"ip\") ")
            if ':' not in master:
                master += ':%s' % main_conf.master.port
        else:
            logger.error('Input illegal!')
    else:
        if ':' not in master:
            master += ':%s' % main_conf.master.port
            
    if master is None:
        logger.error('Master cannot be null.')
    else:
        try:
            runned = False
            
            for name, func in registered_func.iteritems():
                if hasattr(args, name):
                    runned = True
                    params = tuple(getattr(args, name))
                    func(master, *params)
                    
            if not runned:
                logger.info('Nothing to run!')
                    
        except KeyboardInterrupt:
            logger.error('interuptted')
            stop()
        except Exception, e:
            logger.exception(e)
            stop()