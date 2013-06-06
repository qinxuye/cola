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
    def __init__(self, filename, capacity):
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            f = open(filename)
            try:
                lines = f.readlines()
                if len(lines) != 2:
                    raise BloomFilterFileDamage('Bloom filter file has been damaged.')
                
                old_capacity, hash_ = tuple(lines)
                try:
                    old_capacity = int(old_capacity)
                    hash_ = long(hash_)
                except ValueError:
                    raise BloomFilterFileDamage('Bloom filter file must have right hash value.')
                
                if capacity > old_capacity:
                    super(FileBloomFilter, self).__init__(capacity=capacity)
                    del hash_
                    self.capacity = capacity
                else:
                    super(FileBloomFilter, self).__init__(capacity=old_capacity)
                    self.hash = hash_
                    self.capacity = old_capacity
            finally:
                f.close()
        else:
            super(FileBloomFilter, self).__init__(capacity=capacity)
            self.capacity = capacity
        
        self.f = open(filename, 'w+')
        
    def verify(self, item):
        exists = item in self
        if not exists:
            self.add(item)
        return exists
    
    def sync(self):
        self.f.seek(0)
        self.f.writelines([
            str(self.capacity) + '\n',
            str(self.hash)
        ])
    
    def close(self):
        self.f.close()
        
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.close()