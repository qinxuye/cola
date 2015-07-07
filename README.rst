===============================================
Cola: high-level distributed crawling framework
===============================================

.. image:: https://badge.fury.io/py/cola.svg
   :target: http://badge.fury.io/py/cola

Overview
========

**Cola** is a high-level distributed crawling framework, 
used to crawl pages and extract structured data from websites.
It provides simple and fast yet flexible way to achieve your data acquisition objective.
Users only need to write one piece of code which can run under both local and distributed mode.

Requirements
------------

* Python2.7 (Python3+ will be supported later)
* Work on Linux, Windows and Mac OSX

Install
=======

The quick way:

::
  
  pip install cola

Or, download source code, then run:

::
  
  python setup.py install

Write applications
==================

Documents will update soon, now just refer to the 
`wiki <https://github.com/chineking/cola/tree/master/app/wiki>`_ or
`weibo <https://github.com/chineking/cola/tree/master/app/weibo>`_ application.

Run applications
================

For the wiki or weibo app, please ensure the installation of dependencies, weibo as an example:

::

  pip install -r /path/to/cola/app/weibo/requirements.txt

Local mode
----------

In order to let your application support local mode, just add code to the entrance as below.

.. code-block:: python

  from cola.context import Context
  ctx = Context(local_mode=True)
  ctx.run_job(os.path.dirname(os.path.abspath(__file__)))

Then run the application:

::

  python __init__.py
  
Stop the local job by ``CTRL+C``.

Distributed mode
----------------

Start master:

::

  coca master -s [ip:port]

Start one or more workers:

::

  coca worker -s -m [ip:port]

Then run the application(weibo as an example):

::

  coca job -u /path/to/cola/app/weibo -r

Coca command
============

Coca is a convenient command-line tool for the whole cola environment.

master
------

Kill master to stop the whole cluster:

::

  coca master -k

job
---

List all jobs:

::

  coca job -m [ip:port] -l

Example as:

::

  list jobs at master: 10.211.55.2:11103
  ====> job id: 8ZcGfAqHmzc, job description: sina weibo crawler, status: stopped

You can run a job which shown in the list above:

::

  coca job -r 8ZcGfAqHmzc

Actually, you don't have to input the complete job name:

::

  coca job -r 8Z

Part of the job name is fine if there's no conflict.

You can know the status of a running job by:

::

  coca job -t 8Z

The status like counters during running and so on will be output 
to the terminal.

You can kill a job by the kill command:

::

  coca job -k 8Z

startproject
------------

You can create an application by this command:

::

  coca startproject colatest

Remember, help command will always be helpful:

::

  coca -h

or

::

  coca master -h


Notes
=====

`Chinese docs(wiki) <https://github.com/chineking/cola/wiki>`_.

Donation
========

Cola is a non-profit project and by now maintained by myself, 
thus any donation will be encouragement for the further improvements of cola project.

**Alipay & Paypal: qinxuye@gmail.com**