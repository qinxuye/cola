# Cola: high-level distributed crawling framework

## Overview

**Cola** is a high-level distributed crawling framework, 
used to crawl data and extract structured data from pages of websites.
It provides simple and fast yet flexible way to achieve your data acquisition objective.
Users only need to write one piece of code which can run under both local and distributed mode.

## Requirements

* Python2.7 (Python3+ will be supported later)
* Work on Linux, Windows and Mac OSX

## Install

### Source code:

Download source code, then run:

```sh
python setup.py install
```

## Write application

Documents will update soon, now just refer to the 
[wiki](https://github.com/chineking/cola/tree/master/app/wiki) 
or [weibo](https://github.com/chineking/cola/tree/master/app/weibo) application.

## Run application

### Local mode

In order to let your application support local mode, just add code to the entrance as below.

```python
from cola.context import Context
ctx = Context(local_mode=True)
ctx.run_job(os.path.dirname(os.path.abspath(__file__)))
```

Then run the application:

```sh
python __init__.py
```

### Distributed mode

Start master:

```sh
coca master -s [ip:port]
```

Start one or more workers:

```sh
coca worker -s -m [ip:port]
```

Then run the application(weibo as example):

```sh
coca job -u /path/to/cola/app/weibo -r
```

## Coca command

Coca is a convenient command-line tool for the whole cola environment.

### master

Kill master to stop the whole cluster:

```
coca master -k
```

### job

List all jobs:

```sh
coca job -m [ip:port] -l
```

Example as:

```sh
list jobs at master: 10.211.55.2:11103
====> job id: 8ZcGfAqHmzc, job_name: sina weibo crawler, status: stopped
```

You can run a job which shown in the list above:

```sh
coca job -r 8ZcGfAqHmzc
```

Actually, you don't have to input the complete job name:

```sh
coca job -r 8Z
```

Part of the job name is fine if there's no conflict.

You can know the status of a running job by:

```sh
coca job -t 8Z
```

The status like counters during running and so on will be output 
to the terminal.

You can kill a job by the kill command:

```sh
coca job -k 8Z
```

### startproject

You can create an application by this command:

```sh
coca startproject colatest
```

Remember, help command will always be helpful:
 
```sh
coca -h
```

or

```sh
coca master -h
```


## Notes

* [Chinese docs(wiki)](https://github.com/chineking/cola/wiki).