#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A tag based cache system using redis
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Redis based cache system which works with tags. Allows expiration 
    of keys based on tags, for example all users or posts.

    :copyright: (c) 2012 by Michael Mayr.
    :license: ISC, see LICENSE for more details.

    Some notes:
    - This classes uses heavily pipelines, but does not really support atomic transations
    - You should call gc() from time to time (cronjob) to remove
      orphan links between expired entries and tags
"""

class TaggedCache(object):
    """Redis based cache system which works with tags. Allows expiration 
    of keys based on tags, for example all users or posts."""

    def __init__(self, redis):
        assert hasattr(redis, "pipeline"), "Redis instance doesn't support pipelines"
        self.redis = redis

    def set(self, key, value, tags=[], ttl=None):
        """Add or update a cache entry

        key     Name of cache entry
        value   Value
        tags    One or more tags (may contain duplicates)
        ttl     Time to life (Expiration in seconds, optional)
        """

        p = self.redis.pipeline()
        for tag in tags:
            p.sadd("tags-by-key:%s" % key, tag)
            p.sadd("keys-by-tag:%s" % tag, key)
            p.sadd("Tags", tag)
        p.sadd("CacheKeys", key)
        p.set(key, value)
        if ttl:
            p.expire(key, ttl)
        p.execute()

    def get(self, key, gc=True):
        """Return a cached value. Returns None if not found. If the entry
        expired, all assigned tags will be deleted. Disable this behaviour with
        gc=False."""

        value = self.redis.get(key)
        if value is None and gc:
            # Remove all existing tags
            self.gc([key])
        return value

    def get_tags(self, key):
        """Returns all tags used by a key"""

        return self.redis.smembers("tags-by-key:%s" % key)

    def get_keys(self, tag):
        """Returns all cache entries matching a given tag"""

        return self.redis.smembers("keys-by-tag:%s" % tag)

    def clear(self, key):
        """Remove a single key from the cache"""

        self.redis.delete(key)
        self.gc([key])

    def clear_tag(self, tag):
        """Clears all cache entries with belongs to any of the given tags"""

        p = self.redis.pipeline()
        deleted_keys = set()
        for key in self.redis.smembers("keys-by-tag:%s" % tag):
            p.delete(key)
            deleted_keys.add(key)
        p.delete("keys-by-tag:%s" % tag)
        p.execute()
        self.gc(deleted_keys)

    def clear_all(self):
        """Delete all cache entries and reset tag sets"""

        p = self.redis.pipeline()
        for key in self.redis.smembers("CacheKeys"):
            p.delete(key)
            p.delete("tags-by-key:%s" % key)
        for tag in self.redis.smembers("Tags"):
            p.delete("keys-by-tag:%s" % tag)
        p.delete("CacheKeys")
        p.delete("Tags")
        p.execute()

    def gc(self, keys=None, tags=None):
        """Check all tag sets and try to delete orphan entries"""

        keys = keys or []
        tags = tags or []
        if not keys:
            # Check all keys - may take a short while
            keys = self.redis.smembers("CacheKeys")
        if not tags:
            # Check all tags - may take a short while
            tags = self.redis.smembers("Tags")

        p = self.redis.pipeline()

        # Delete tag links for expired keys
        for key in keys:
            exists = self.redis.exists(key)
            if not exists:
                p.srem("CacheKeys", key)
                for tag in self.redis.smembers("tags-by-key:%s" % key):
                    p.srem("keys-by-tag:%s" % tag, key)
                p.delete("tags-by-key:%s" % key)

        # Remove sets for unused tags
        for tag in tags:
            num_keys = self.redis.scard("keys-by-tag:%s" % tag)
            if num_keys == 0:
                p.srem("Tags", tag)
                p.delete("keys-by-tag:%s" % tag)

        p.execute()
