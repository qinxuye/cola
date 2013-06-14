cola
====

Cola is a distributed crawling framework. 

Why named cola? hmm, I like cola, and cola sounds a bit like crawler.

##Quick Start

* download or clone source code, add cola to python path.
* start cola master: /path/to/cola/bin/start_master.py
* start cola worker: /path/to/cola/bin/start_worker.py --master [ip address]
* run job: /path/to/cola/bin/coca.py -runLocalJob /path/to/cola/contrib/wiki

##Tips

* [Chinese docs(wiki)](https://github.com/chineking/cola/wiki).
* I am trying my best to make cola stable.
* Cola can also run in a single machine, you don't need to start master, workers and so on. Everything is simple!