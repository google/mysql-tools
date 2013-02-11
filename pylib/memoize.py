# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A decorator for function call memoization.

Multiple calls to the memoized function with the same arguments will not cause
the results to be computed every time. Instead, they are returned from a cache.
"""

__author__ = 'mqian@google.com (Mike Qian)'

import functools
import logging
import threading
import weakref

import cache


_MEMOIZED_FUNCTIONS = weakref.WeakKeyDictionary()


def FlushAllCaches():
  """Flushes all memoized caches to reclaim memory."""
  for memo_ref in _MEMOIZED_FUNCTIONS.values():
    memo = memo_ref()
    if memo is not None:
      memo.FlushCache()


class _CacheValue(object):
  """A value in the cache. Not thread-safe.

  Attributes:
    _stored: a boolean that indicates whether the value is valid
    _value: the actual cached value (only valid if _stored is True)
  """
  __slots__ = ('_stored', '_value')

  def __init__(self):
    self._stored = False
    self._value = None

  def Set(self, value):
    """Loads a given value into this object.

    Args:
      value: Any Python object.
    """
    self._stored = True
    self._value = value

  def MaybeGet(self):
    """Reads the value from this object, if there is a value.

    Returns:
      A tuple (status, value) where:
        status: True if there is a value in this object, False otherwise
        value: The actual value of the object, or None if it does not exist
    """
    return (self._stored, self._value)


class Memoize(object):
  """Decorator to memoize a function."""

  def __init__(self, **kwargs):
    """Creates a memoizing decorator.

    Arguments have to be passed as keywords.

    Args:
      timeout: The amount of time in seconds (float) after which a cached value
        is considered stale and will not be used anymore. In such a case, the
        memoized function is called again. If None, then values never go stale.

    Raises:
      TypeError: You provided unsuported arguments.
    """
    timeout = kwargs.pop('timeout', None)
    assert timeout is None or timeout > 0, 'timeout must be > 0 or None'
    if kwargs:
      raise TypeError('Invalid arguments: %s' % ', '.join(kwargs.keys()))

    self._cache = cache.SimpleCache(max_age=timeout)
    self._func = None
    self._lock = threading.Lock()

  def __call__(self, func):
    """Returns the memoized version of func."""

    def ReturnedFunc(*args, **kwargs):
      return self._MemoizedFunction(*args, **kwargs)

    assert self._func is None, 'You cannot reuse this decorator'
    self._func = func
    _MEMOIZED_FUNCTIONS[ReturnedFunc] = weakref.ref(self)

    # Try to copy over some attributes of the original function.
    # Some assignments may fail if the function is a method of an object.
    try:
      ReturnedFunc.__name__ = func.__name__
      self._func_name = func.__name__
    except AttributeError:
      # If the object is callable, try the class name instead.
      try:
        ReturnedFunc.__name__ = func.__class__.__name__
        self._func_name = func.__class__.__name__
      except AttributeError:
        self._func_name = '<unknown>'
    try:
      ReturnedFunc.__dict__ = dict(func.__dict__)
    except AttributeError:
      pass
    try:
      ReturnedFunc.__doc__ = func.__doc__
    except AttributeError:
      pass
    try:
      ReturnedFunc.__module__ = func.__module__
    except AttributeError:
      pass

    ReturnedFunc.FlushCache = self.FlushCache
    return ReturnedFunc

  def _CacheKey(self, *args, **kwargs):
    """Compute the memoized function's cache key.

    A function call is keyed by its positional and keyword arguments.
    """
    return (tuple(args), frozenset(kwargs.iteritems()))

  def FlushCache(self):
    """Flushes the memoized cache for all argument values.

    This call clears the underlying data structures of the cache, so you can
    use this to reclaim memory.
    """
    with self._lock:
      self._cache.Flush()

  def _MemoizedFunction(self, *args, **kwargs):
    """Returns the value of a memoized function call.

    If a value for the same function and call arguments remains in the cache,
    this function will return that value. Otherwise, it will call the function
    again, caching and returning the result.
    """
    # In case the arguments are not hashable.
    try:
      key = self._CacheKey(*args, **kwargs)
    except TypeError, e:
      logging.warn('Unhashable args for %s(args=%s, kwargs=%s), cannot '
                   'memoize: %s', self._func_name, args, kwargs, e)
      return self._func(*args, **kwargs)

    # Get the corresponding cache element, or build one if it does not exist.
    with self._lock:
      cache_entry = self._cache.Get(key)
      if cache_entry is None:
        cache_entry = _CacheValue()
        self._cache.Put(key, cache_entry)

    # Try to read from the cache. If it is a hit, return the cached value.
    with self._lock:
      status, value = cache_entry.MaybeGet()
      if status:
        logging.debug('Returning cached value for %s(args=%s, kwargs=%s)',
                      self._func_name, args, kwargs)
        return value

    # Otherwise, call the memoized function again.
    return_value = self._func(*args, **kwargs)
    with self._lock:
      cache_entry.Set(return_value)
    return return_value
