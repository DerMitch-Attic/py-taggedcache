#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A tag based cache system using redis
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Redis based cache system which works with tags. Allows expiration 
    of keys based on tags, for example all users or posts.

    :copyright: (c) 2012 by Michael Mayr.
    :license: ISC, see LICENSE for more details.

    WARNING: Run the test suite against a standalone redis server, cause
    it will flush (= destroy) the whole database (3) before running!

    Run tests with coverage:
    # coverage run --source=. --branch test.py; coverage report -m
"""

import time
import unittest

from redis import StrictRedis

from taggedcache import TaggedCache

DATABASE = 3

class testTaggedCache(unittest.TestCase):
    def setUp(self):
        self.redis = StrictRedis(db=DATABASE)
        self.redis.flushdb()
        self.cache = TaggedCache(self.redis)

    def testEverything(self):
        cache = self.cache

        # Set
        cache.set("User:1", "mitch", tags=["User", "PremiumUser"])
        cache.set("User:2", "foo", tags=["User"])
        cache.set("Post:1", "Hello World!", tags=["Post"])
        cache.set("Post:2", "Hello World, again!", tags=["Post"])
        
        self.assertEquals(cache.get("Post:1"), "Hello World!")
        self.assertEquals(cache.get_keys("Post"), set(["Post:1", "Post:2"]))
        self.assertEquals(cache.get_keys("User"), set(["User:1", "User:2"]))
        self.assertEquals(cache.get_tags("User:1"), set(["User", "PremiumUser"]))
        self.assertEquals(cache.get_tags("User:2"), set(["User"]))

        # Delete all post cache entries
        cache.clear_tag("Post")
        self.assertEquals(cache.get("Post:1"), None, "Post:1 still exists")
        self.assertEquals(cache.get("Post:2"), None, "Post:2 still exists")
        
        # Delete User 2 from cache
        cache.clear("User:2")
        self.assertEquals(cache.get_tags("User:2"), set())

        # Clear everything else
        cache.clear_all()
        cache.gc()

        self.assertEquals(self.redis.get("CacheKeys"), None)
        self.assertEquals(self.redis.get("Tags"), None)
        
        self.assertEquals(len(self.redis.keys("*")), 0, "Some keys were not gc'ed")

    def testExpireTtl(self):
        self.cache.set("ExpireMe", "foo", ttl=1)
        time.sleep(2)
        self.assertEquals(self.cache.get("ExpireMe"), None)

if __name__ == '__main__':
    unittest.main()
