#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc.
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

"""Tools for advanced thread use in Python."""

import functools
import heapq
import logging
import Queue
import os
import signal
import threading
import time
import weakref


def DoNothing():
  pass


class BlockingCallback(object):
  """Encapsulate a callback while allowing blocking until it completes."""

  def __init__(self, callback, num_calls=1):
    """Constructor.

    It is an error to call the return value more than num_calls times.

    Args:
      callback: Function to call after we've been called num_calls times.
      num_calls: Number of calls to wait for.

    Returns:
      An object that is also callable.
    """
    self._callback = callback
    self._calls_remaining = num_calls
    self._lock = threading.Lock()
    self._event = threading.Event()

  def __call__(self, *args, **kwargs):
    """Make this object magically callable."""
    with self._lock:
      self._calls_remaining -= 1
      if self._calls_remaining > 0:
        return
      assert self._calls_remaining == 0
    ret = None
    if self._callback:
      ret = self._callback(*args, **kwargs)
    self._event.set()
    return ret

  def Wait(self):
    """Block until called num_calls times and the callback completes."""
    self._event.wait()


class CancellableCallback(object):
  """Encapsulate a callback while allowing cancellation."""

  _cancelled = False

  def __init__(self, callback):
    self._callback = callback

  def __call__(self, *args, **kwargs):
    if self._cancelled:
      return
    return self._callback(*args, **kwargs)

  def Cancel(self):
    self._cancelled = True


def ReturnOnReferenceError(func):
  """Return immediately if a weak reference target goes away."""
  def wrapper(*args, **kwargs):
    try:
      func(*args, **kwargs)
    except ReferenceError:
      logging.exception('Reference went bad; returning')
      return
  return wrapper


def AbortOnException(func):
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except Exception:
      logging.exception('Exception in callback.')
      os.kill(os.getpid(), signal.SIGABRT)
  return wrapper


class EventManager(object):
  """Run tasks on a thread pool, either ASAP or after some number of seconds.

  There is no inter-task priority. Tasks are run first-come, first-served,
  though they do not necessarily complete in that order. Tasks added with Add()
  may block tasks added with AddAfter(), and vice versa.

  Example:
    def foo():
      print 'foo'
    em = EventManager(4)
    em.Add(foo)
    em.AddAfter(2.5, foo)
    ...
    em.Shutdown()
  """

  _shutdown = True
  _scheduler = None

  def __init__(self, num_threads, max_queue_size=0):
    """Initialize.

    Args:
      num_threads: Number of threads to start, not including donated threads.
      max_queue_size: Maximum number of tasks pending ASAP execution, before
        Add() blocks and scheduling becomes unreliable.
    """
    self._num_threads = num_threads
    self._threads = []
    # Expects callback in queue.
    self._run_queue = Queue.Queue(max_queue_size)
    # Expects (time_to_run, callback) in queue.
    self._schedule_queue = Queue.Queue()
    self._shutdown_lock = threading.Lock()

  def __del__(self):
    self.Shutdown()

  def Start(self):
    """Start all worker threads."""
    assert not self._threads, 'EventManager started when already running.'
    self._shutdown = False
    # Threads get weak references to the instance so they don't block
    # destruction, since we'll tear them down during destruction anyway. This
    # does mean that self can suddenly become invalid inside _Worker() and
    # _Scheduler().
    weakself = weakref.proxy(self)
    self._scheduler = threading.Thread(target=EventManager._Scheduler,
                                       args=(weakself,))
    self._scheduler.start()
    for _ in xrange(self._num_threads):
      t = threading.Thread(target=EventManager._Worker, args=(weakself,))
      t.start()
      self._threads.append(t)

  def StartShutdown(self):
    """Signal all threads to shut down (at some point in the future).

    This is safe to call from within an EventManager callback.
    """
    self._shutdown = True
    # Wake up all the threads, so they notice the shutdown flag.
    self.AddAfter(0, DoNothing)
    for _ in self._threads:
      self.Add(DoNothing)

  def Shutdown(self):
    """Shutdown all threads after they finish their current task.

    This will raise RuntimeError if called from within an EventManager callback,
    leaving the EventManager in an undefined state.
    """
    self.StartShutdown()
    self._shutdown_lock.acquire()
    # Shut down the scheduler.
    if self._scheduler:
      self._scheduler.join()
      self._scheduler = None
    # Wait for worker threads to finish their current task.
    try:
      while True:
        t = self._threads.pop()
        if isinstance(t, threading.Event):
          t.wait()
        else:
          assert isinstance(t, threading.Thread)
          t.join()
    except IndexError:
      # We ran out of threads to pop.
      pass
    self._shutdown_lock.release()

  @ReturnOnReferenceError
  def _Worker(self):
    """Worker thread main function."""
    logging.info('Worker thread starting')
    while True:
      callback = self._run_queue.get(True)
      if self._shutdown:
        logging.info('Worker thread shutting down')
        break
      try:
        callback()
      except Exception:
        logging.exception('Exception inside %s', callback)

  @ReturnOnReferenceError
  def _Scheduler(self):
    """Scheduler thread main function."""
    # (time_to_run, callback)
    schedule = []  # actually an empty heapq
    while True:
      if schedule:
        timeout = max(0.0, schedule[0][0] - time.time())
      else:
        timeout = None
      try:
        entry = self._schedule_queue.get(True, timeout)
        heapq.heappush(schedule, entry)
      except Queue.Empty:
        # We woke up to execute a scheduled event, not because we had new
        # input.
        pass
      if self._shutdown:
        break
      while schedule and schedule[0][0] <= time.time():
        self.Add(heapq.heappop(schedule)[1])

  def Add(self, callback, *args, **kwargs):
    """Add a callback to be run on the thread pool."""
    self._run_queue.put(functools.partial(callback, *args, **kwargs))

  def AddAfter(self, delay, callback, *args, **kwargs):
    """Add a callback to be run on the thread pool after delay seconds.

    We guarantee that callback will not be run before delay seconds have
    elapsed, but make no guarantees about how much time will pass after that
    point before it is started.
    """
    assert delay >= 0
    self._schedule_queue.put((time.time() + delay,
                              functools.partial(callback, *args, **kwargs)))

  def DonateThread(self):
    """Add the current thread to the thread pool.

    Blocks until something calls Shutdown() or StartShutdown().
    """
    event = threading.Event()
    self._threads.append(event)
    self._Worker()
    event.set()

  def Partial(self, func, *args, **kwargs):
    """Like functools.partial, but will run the callback in this EventManager.

    Note that this makes the callback non-blocking.
    """
    callback = functools.partial(func, *args, **kwargs)
    def Callback(*args2, **kwargs2):
      self.Add(callback, *args2, **kwargs2)
    return Callback
