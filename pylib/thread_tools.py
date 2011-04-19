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

import heapq
import Queue
import threading
import time
import weakref


def DoNothing():
  pass


class Notification(object):
  """Allow multiple threads to wait for an occurrence of an event.

  All threads block on WaitForNotification() until one thread calls Notify(),
  after which all other calls unblock.
  """

  def __init__(self):
    self._lock = threading.Lock()
    self._lock.acquire()

  def Notify(self):
    self._lock.release()

  def WaitForNotification(self):
    self._lock.acquire()
    self._lock.release()

  def HasBeenNotified(self):
    if self._lock.acquire(False):
      self._lock.release()
      return True
    else:
      return False


class BlockingCallback(object):
  """Encapsulate a callback while allowing blocking until it completes."""

  def __init__(self, callback):
    self._callback = callback
    self._notification = Notification()

  def __call__(self, *args, **kwargs):
    ret = self._callback(*args, **kwargs)
    self._notification.Notify()
    return ret

  def Wait(self):
    self._notification.WaitForNotification()


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
      return
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
    """Initialize and start worker threads.

    Args:
      num_threads: See Start().
      max_queue_size: Maximum number of tasks pending ASAP execution, before
        Add() blocks and scheduling becomes unreliable.
    """
    self._threads = []
    # Expects callback in queue.
    self._run_queue = Queue.Queue(max_queue_size)
    # Expects (time_to_run, callback) in queue.
    self._schedule_queue = Queue.Queue()
    self._shutdown_lock = threading.Lock()
    self.Start(num_threads)

  def __del__(self):
    self.Shutdown()

  def Start(self, num_threads):
    """Start all worker threads.

    Args:
      num_threads: Number of threads to start, not including donated threads.
    """
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
    for _ in xrange(num_threads):
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
        if isinstance(t, Notification):
          t.WaitForNotification()
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
    while True:
      callback = self._run_queue.get(True)
      if self._shutdown:
        break
      callback()

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

  def Add(self, callback):
    """Add a callback to be run on the thread pool."""
    self._run_queue.put(callback)

  def AddAfter(self, delay, callback):
    """Add a callback to be run on the thread pool after delay seconds.

    We guarantee that callback will not be run before delay seconds have
    elapsed, but make no guarantees about how much time will pass after that
    point before it is started.
    """
    assert delay >= 0
    self._schedule_queue.put((time.time() + delay, callback))

  def DonateThread(self):
    """Add the current thread to the thread pool.

    Blocks until something calls Shutdown() or StartShutdown().
    """
    notification = Notification()
    self._threads.append(notification)
    self._Worker()
    notification.Notify()
