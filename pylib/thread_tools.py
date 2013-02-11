# Copyright 2011 Google Inc. All Rights Reserved.
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
import os
import Queue
import signal
import threading
import time


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
    self._exception = None
    self._result = None

  def __call__(self, *args, **kwargs):
    """Make this object magically callable."""
    with self._lock:
      self._calls_remaining -= 1
      if self._calls_remaining > 0:
        return
      assert self._calls_remaining == 0
    try:
      if self._callback:
        self._result = self._callback(*args, **kwargs)
        # reset any previous exception now.
        self._exception = None
    except Exception as err:
      self._exception = err
      self._result = None
      # this does not change the old semantics
      # result can be obtained from .GetResult().
      raise
    finally:
      self._event.set()
    return self._result

  def GetResult(self):
    """Get the result of the latest callback call.

    If the operation is incomplete the return value will be None.  If the
    operation raised an exception, the same exception will be raised here.

    Returns:
      The result of the latest callback call.
    """
    if self._exception:
      raise self._exception
    return self._result

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


def AbortOnException(func):

  def Wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except Exception:
      logging.exception('Exception in callback.')
      os.kill(os.getpid(), signal.SIGABRT)
  return Wrapper


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

  class EMState(object):
    """Store shared state between EventManager and child threads."""
    def __init__(self, max_queue_size):
      # Immediate events are just callbacks.
      self.run_queue = Queue.Queue(max_queue_size)
      # Scheduled events are (time_to_run, callback) tuples.
      self.schedule_queue = Queue.Queue()
      # Actually an empty heapq.
      self.schedule = []
      self.shutdown = True

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
    self._state = self.EMState(max_queue_size)
    self._shutdown_lock = threading.Lock()

  def __del__(self):
    self.Shutdown()

  def Start(self):
    """Start all worker threads."""
    assert not self._threads, 'EventManager started when already running.'
    self._state.shutdown = False
    # Only the EMState object is shared between this object and its child
    # threads. This allows our reference count to go to zero and destruction to
    # occur.
    self._scheduler = threading.Thread(target=self._Scheduler,
                                       args=(self._state,))
    self._scheduler.start()
    for _ in xrange(self._num_threads):
      t = threading.Thread(target=self._Worker, args=(self._state,))
      t.start()
      self._threads.append(t)

  def StartShutdown(self):
    """Signal all threads to shut down (at some point in the future).

    This is safe to call from within an EventManager callback.
    """
    self._state.shutdown = True
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
    with self._shutdown_lock:
      # Shut down the scheduler.
      if self._scheduler:
        self._scheduler.join()
        self._scheduler = None
      # Wait for worker threads to finish their current task.
      try:
        while True:
          t = self._threads.pop()
          if isinstance(t, threading._Event):
            t.wait()
          else:
            assert isinstance(t, threading.Thread)
            t.join()
      except IndexError:
        # We ran out of threads to pop.
        pass

  def Wait(self):
    """Wait for the queue of immediate tasks to empty."""
    self._state.run_queue.join()

  @staticmethod
  def _Worker(state):
    """Worker thread main function."""
    logging.info('Worker thread starting')
    while True:
      if state.shutdown:
        logging.info('Worker thread shutting down')
        break
      callback = state.run_queue.get(True)
      # It would be good to check shutdown here, but then we may be stuck
      # holding a callback that we can't re-add to the queue (since it would
      # reorder, and it might be full and deadlock).
      try:
        callback()
      except Exception:
        logging.exception('Exception inside %s', callback)
      finally:
        state.run_queue.task_done()

  @staticmethod
  def _Scheduler(state):
    """Scheduler thread main function."""
    while True:
      if state.schedule:
        timeout = max(0.0, state.schedule[0][0] - time.time())
      else:
        timeout = None
      try:
        entry = state.schedule_queue.get(True, timeout)
        heapq.heappush(state.schedule, entry)
      except Queue.Empty:
        # We woke up to execute a scheduled event, not because we had new
        # input.
        pass
      if state.shutdown:
        break
      while state.schedule and state.schedule[0][0] <= time.time():
        state.run_queue.put(heapq.heappop(state.schedule)[1])

  def Add(self, callback, *args, **kwargs):
    """Add a callback to be run on the thread pool."""
    self._state.run_queue.put(functools.partial(callback, *args, **kwargs))

  def AddAfter(self, delay, callback, *args, **kwargs):
    """Add a callback to be run on the thread pool after delay seconds.

    Args:
      delay: delay in seconds before callback is invoked.
      callback: the callback to be invoked.
      *args: positional arguments passed to the callback.
      **kwargs: keyword arguments passed to the callback.

    We guarantee that callback will not be run before delay seconds have
    elapsed, but make no guarantees about how much time will pass after that
    point before it is started.
    """
    assert delay >= 0, 'Delay must be non-negative'
    self._state.schedule_queue.put(
        (time.time() + delay,
         functools.partial(callback, *args, **kwargs)))

  def AddPeriodic(self, delay, callback, *args, **kwargs):
    """Add a callback to be run on the thread pool periodically.

    Args:
      delay: delay in seconds between successive invocations of the callback
      callback: the callback to be invoked.
      *args: positional arguments passed to the callback.
      **kwargs: keyword arguments passed to the callback.

    We guarantee that callback will not be run before delay seconds have
    elapsed since the last call finished, but make no guarantees about how much
    time will pass after that point before another call is made.
    """
    assert delay >= 0, 'Delay must be non-negative'
    def Callback():
      try:
        callback(*args, **kwargs)
      finally:
        self.AddAfter(delay, Callback)
    self.Add(Callback)

  def DonateThread(self):
    """Add the current thread to the thread pool.

    Blocks until something calls Shutdown() or StartShutdown().
    """
    event = threading.Event()
    self._threads.append(event)
    self._Worker(self._state)
    event.set()

  def Partial(self, func, *args, **kwargs):
    """Like functools.partial but will run the callback in this EventManager.

    Creates a function with early-bound arguments like
    functools.partial.  When invoked, the function will add the
    callback to this EventManager.  Arguments to the callback can be
    supplied as arguments to this function (early binding), or
    supplied at the time the callback is invoked (late binding).

    Args:
      func: the function to be invoked as a callback.
      *args: positional arguments passed to the callback.
      **kwargs: keyword arguments passed to the callback.

    Returns:
      A function that, when invoked with positional and keyword
      arguments, will add the callback to the event manager.

    Note that this makes the callback non-blocking.
    """
    callback = functools.partial(func, *args, **kwargs)

    def Callback(*args2, **kwargs2):
      self.Add(callback, *args2, **kwargs2)
    return Callback
