#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
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

Implementation of a Bloom filter in Python.

The Bloom filter is a space-efficient probabilistic data structure that is
used to test whether an element is a member of a set. False positives are
possible, but false negatives are not. Elements can be added to the set, but 
not removed. The more elements that are added to the set, the larger the
probability of false positives.

Uses SHA-1 from Python's hashlib, but you can swap that out with any other
160-bit hash function. Also keep in mind that it starts off very sparse and
become more dense (and false-positive-prone) as you add more elements.

Modified from part of python-hashes by sangelone.
"""

import math
import hashlib
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.bloomfilter.hashtype import HashType


class BloomFilter(HashType):
    def __init__(self, value='', capacity=3000, false_positive_rate=0.01):
        """
        'value' is the initial string or list of strings to hash,
        'capacity' is the expected upper limit on items inserted, and
        'false_positive_rate' is self-explanatory but the smaller it is, the larger your hashes!
        """
        self.create_hash(value, capacity, false_positive_rate)

    def create_hash(self, initial, capacity, error):
        """
        Calculates a Bloom filter with the specified parameters.
        Initalizes with a string or list/set/tuple of strings. No output.

        Reference material: http://bitworking.org/news/380/bloom-filter-resources
        """
        self.hash = 0L
        self.hashbits, self.num_hashes = self._optimal_size(capacity, error)

        if len(initial):
            if type(initial) == str:
                self.add(initial)
            else:
                for t in initial:
                    self.add(t)
    
    def _hashes(self, item):
        """
        To create the hash functions we use the SHA-1 hash of the
        string and chop that up into 20 bit values and then
        mod down to the length of the Bloom filter.
        """
        m = hashlib.sha1()
        m.update(item)
        digits = m.hexdigest()
    
        # Add another 160 bits for every 8 (20-bit long) hashes we need
        for i in range(self.num_hashes / 8):
            m.update(str(i))
            digits += m.hexdigest()
    
        hashes = [int(digits[i*5:i*5+5], 16) % self.hashbits for i in range(self.num_hashes)]
        return hashes  

    def _optimal_size(self, capacity, error):
        """Calculates minimum number of bits in filter array and
        number of hash functions given a number of enteries (maximum)
        and the desired error rate (falese positives).
        
        Example:
            m, k = self._optimal_size(3000, 0.01)   # m=28756, k=7
        """
        m = math.ceil((capacity * math.log(error)) / math.log(1.0 / (math.pow(2.0, math.log(2.0)))))
        k = math.ceil(math.log(2.0) * m / capacity)
        return (int(m), int(k))

    
    def add(self, item):
        "Add an item (string) to the filter. Cannot be removed later!"
        for pos in self._hashes(item):
            self.hash |= (2 ** pos)

    def __contains__(self, name):
        "This function is used by the 'in' keyword"
        retval = True
        for pos in self._hashes(name):
            retval = retval and bool(self.hash & (2 ** pos))
        return retval

class BloomFilterFileDamage(Exception): pass

class FileBloomFilter(BloomFilter):
    def __init__(self, filename, capacity, false_positive_rate=0.01):
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename) as f:
                old_capcity, old_false_rate, hash_ = pickle.load(f)
                    
                if capacity > old_capcity or \
                    false_positive_rate < old_false_rate:
                    del hash_
                    self.capacity, self.false_positive_rate = \
                        capacity, false_positive_rate
                else:
                    self.capacity = old_capcity
                    self.false_positive_rate = old_false_rate
                
                super(FileBloomFilter, self).__init__(
                    capacity=self.capacity,
                    false_positive_rate=self.false_positive_rate)
                if 'hash_' in locals():
                    self.hash = hash_
        else:
            self.capacity = capacity
            self.false_positive_rate = false_positive_rate
            super(FileBloomFilter, self).__init__(
                capacity=capacity, 
                false_positive_rate=false_positive_rate)
        
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.f = open(filename, 'w+')
        
    def verify(self, item):
        exists = item in self
        if not exists:
            self.add(item)
        return exists
    
    def sync(self):
        pickle.dump((self.capacity, self.false_positive_rate, self.hash), 
                    self.f)
    
    def close(self):
        self.f.close()
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.close()