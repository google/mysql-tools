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

"""A pure-Python implementation of a simple cache with expiration."""

__author__ = 'mqian@google.com (Mike Qian)'

import time


class CacheElement(object):
  """Object which holds a single element of cached data in memory."""

  def __init__(self, key, value):
    """Initialize with the given key and value."""
    self._key = key
    self._value = None
    self._timestamp = None
    self.SetValue(value)

  def IsTooOld(self, max_age):
    """Checks if the element's value is more than max_age seconds old.

    If max_age is None, this method always returns False.
    """
    if self._timestamp is None or max_age is None:
      return False
    return time.time() - self._timestamp > max_age

  def GetKey(self):
    """Return the key of the element."""
    return self._key

  def GetTimestamp(self):
    """Return the timestamp of the element."""
    return self._timestamp

  def GetValue(self):
    """Return the current value of the element."""
    if self._timestamp is None: return None
    return self._value

  def SetValue(self, value):
    """Set the element's value to the one given."""
    self._value = value
    self._timestamp = time.time()

  key = property(lambda self: self.GetKey())
  timestamp = property(lambda self: self.GetTimestamp())


class SimpleCache(object):
  """The cache itself."""

  def __init__(self, max_age=None):
    """Initialize the cache.

    Args:
      max_age: The maximum age of an item, in seconds (float), before the data
        is considered to be invalid.
    """
    self._max_age = max_age
    self._dict = {}

  def Get(self, key):
    """Returns the value of a key in the cache (or None)."""
    if key not in self._dict or self._dict[key].IsTooOld(self._max_age):
      return None
    return self._dict[key].GetValue()

  def Put(self, key, value):
    """Sets the value of a key in the cache.

    If an element with the same key already exists, updates the existing value.
    Otherwise, creates a new element with the given key and value.
    """
    if key in self._dict:
      self._dict[key].SetValue(value)
    else:
      self._dict[key] = CacheElement(key, value)

  def Flush(self):
    """Evicts all values in the cache to make it empty."""
    self._dict.clear()
