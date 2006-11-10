#!/usr/bin/python2.4
#
# Copyright (C) 2006 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A library for performing arbitrary operations within a thread pool.

Operations to be performed are represented by subclasses of the
ThreadPoolOperation class that provide a Run() method to perform the
desired operation.

The ThreadPool class provides a pool of worker threads.  New operations 
are farmed out to the worker threads to be performed.

A thread pool can be created and started with:

  # create a pool of 10 workers
  tp = ThreadPool(10)
  tp.Start()

The ThreadPool class provides are two ways to submit operations.  First,
you can define your own ThreadPoolOperation subclass and pass an instance
object to Submit().  For example:

  # operation class
  class MyOperation(thread_pool.ThreadPoolOperation):
    def __init__(self, arg1, arg2):
      thread_pool.ThreadPoolOperation.__init__(self, 'adding %d and %d' %
                                                     (arg1, arg2))
      self._arg1 = arg1
      self._arg2 = arg2
    def Run(self, pool):
      return self._arg1 + self._arg2

  # submit an operation
  op = MyOperation('foo', 1, 2)
  tp.Submit(op)
  op.Wait()
  print 'result for operation %s is %s' % (op, op.Result())

Alternatively, you can pass any callable object and some arguments to
the Submit() method, and they will be wrapped in a ThreadPoolOperation
subclass for you.  For example:

  # operation function
  def PerformOperation(pool, arg1, arg2):
    return arg1 + arg2

  # submit an operation
  op = tp.Submit(PerformOperation, args=(1, 2))
  op.Wait()
  print 'result for operation %s is %s' % (op, op.Result())

Regardless of which way you submit the operation, you can specify a
queue on which the operation object will be placed when the operation
has been completed.  For example:

  queue = Queue.Queue()
  tp.Submit(PerformOperation, args=(1, 2), queue=queue)
  # ... do other stuff ...
  op = queue.get()
  print 'result for operation %s is %s' % (op, op.Result())

"""

import Queue
import sys
import threading
import time

import compat_logging as logging


###############################################################################
###  exception class
###############################################################################

class ThreadPoolError(Exception):
  pass


###############################################################################
###  class ThreadPoolOperation
###############################################################################

class ThreadPoolOperation:
  """
  A base class for operations performed by the thread pool.

  Subclasses should override the Run() method to define the operation
  to be performed.
  """

  def __init__(self, name=None, queue=None):
    """
    Initializer.

    The name argument is a human-readable name for this operation.

    The queue argument, if not None, is a Queue.Queue object on which
    the operation object will be placed when the operation has been
    performed.
    """
    if name is None:
      self._name = repr(self)
    else:
      self._name = name
    self._queue = queue
    self._done = threading.Event()
    self._result = None

  def __str__(self):
    return self._name

  def _Queue(self):
    """
    Returns the queue that the completed operation should be placed on.
    For use by the ThreadPool class.
    """
    return self._queue

  def _DoOperation(self):
    """
    A wrapper around the Run() method that provides exception handling
    and saves the operation's result.

    For use by the _ThreadPoolWorker class.
    """
    try:
      self._result = self.Run()
    except Exception, e:
      logging.error('caught exception: %s' % e)
      # set result to exception object
      self._result = e
    self._done.set()

  def Run(self):
    """
    This is the method that actually performs the operation.  It must
    be implemented by each subclass.

    The return value will be saved and will be available via the Result()
    method.
    """
    raise NotImplementedError('pure virtual method called')

  def Result(self):
    """
    Returns the operation's result.
    """
    return self._result

  def Wait(self, timeout=None):
    """
    Blocks until either the operation is completed or the timeout expires.
    """
    self._done.wait(timeout)


###############################################################################
###  class _FunctionOperation
###############################################################################

class _FunctionOperation(ThreadPoolOperation):
  """
  An operation class for calling a function.  This is used by
  ThreadPool.Submit().
  """

  def __init__(self, func, args=(), name=None, queue=None):
    """
    Initializer.

    Arguments:
      func        - a callable object that performs the operation
      args        - a list or tuple containing additional args to be
                    passed to func (note: the ThreadPool object will
                    be passed as the first argument)
      name        - an optional name for the operation
      queue       - an optional Queue.Queue object on which the
                    operation object will be placed when it has been
                    performed
    """
    ThreadPoolOperation.__init__(self, name=name, queue=queue)
    self._func = func
    self._args = args

  def Run(self):
    """
    Calls the specified function with the the specified args.
    """
    return self._func(*self._args)


###############################################################################
###  class _ThreadPoolWorker
###############################################################################

class _ThreadPoolWorker(threading.Thread):
  """
  A worker thread.
  """

  def __init__(self, pool_proxy, name, sleep_time):
    threading.Thread.__init__(self)
    self.setDaemon(1)
    self.setName(name)
    self._pool = pool_proxy
    self._sleep_time = sleep_time

  def run(self):
    while not self._pool._TerminationRequested():
      logging.debug('[%s] checking dispatch queue', self.getName())
      # TODO: should abstract away Queue.Empty so that people can
      # subclass _ThreadPoolWorker without needing to know internal
      # details of ThreadPool implementation
      try:
        op = self._pool._GetNextOperation()
      except Queue.Empty:
        logging.debug('[%s] dispatch queue empty - sleeping for %f',
                      threading.currentThread().getName(), self._sleep_time)
        time.sleep(self._sleep_time)
        continue
      
      logging.debug('[%s] starting operation: %s', self.getName(), op)
      op._DoOperation()
      logging.debug('[%s] finished operation: %s', self.getName(), op)
      self._pool._DoneWithOperation(op)
    # terminate thread
    logging.debug('[%s] worker thread terminating', self.getName())
    sys.exit()


###############################################################################
###  class _Counter
###############################################################################

class _Counter:
  """
  A thread-safe wrapper around a simple integer counter.
  """

  def __init__(self):
    self._count = 0
    self._lock = threading.Lock()

  def Increment(self):
    self._lock.acquire()
    try:
      self._count += 1
    finally:
      self._lock.release()

  def Decrement(self):
    self._lock.acquire()
    try:
      self._count -= 1
    finally:
      self._lock.release()

  def Value(self):
    self._lock.acquire()
    try:
      return self._count
    finally:
      self._lock.release()


###############################################################################
###  class ThreadPool
###############################################################################

class ThreadPool:
  """
  Implements a thread pool for performing aribitrary operations.

  Each operation that needs to be performed is represented by an
  ThreadPoolOperation object.  New subclasses of ThreadPoolOperation
  can be created for different operation types.

  A pool of worker threads is created to perform operations.  A dispatch
  queue is used to send operations to the thread pool for execution,
  and a result queue is used to return the result to the main thread.
  """

  # TODO: allow dynamic resizing

  # TODO: the semantics should be changed so that pending ops are only
  # those ops that have not yet been *started* by a worker thread (as
  # opposed to those that have not yet been *finished*)

  def __init__(self, num_workers, sleep_time=1.0):
    """
    Initializer.

    The num_workers argument must be an integer indicating the number of
    worker threads to be spawned.

    The optional argument sleep_time is a float indicating the amount of
    time that worker threads should sleep between attempts to get an
    operation from the dispatch queue.
    """
    self._num_workers = num_workers
    self._workers = [ ]
    self._sleep_time = sleep_time
    self._dispatch_queue = Queue.Queue(0)
    self._num_pending_ops = _Counter()
    self._num_finished_ops = _Counter()
    self._running = threading.Event()
    self._termination_requested = threading.Event()
    self._proxy = _ThreadPoolProxy(self._num_pending_ops,
                                   self._num_finished_ops,
                                   self._termination_requested,
                                   self._dispatch_queue)

  def __del__(self):
    # We have to join on the threads in order not to leak memory.
    if self._running.isSet():
      self.Shutdown()

  def Start(self):
    """
    Spawn the worker threads and begin allowing new operations to be
    submitted.  Raises a ThreadPoolError exception if the pool is
    already running.
    """
    if self._running.isSet():
      raise ThreadPoolError('thread pool already running')

    logging.debug('starting thread pool...')

    # make sure we're not requesting termination so that the worker
    # threads don't die immediately
    self._termination_requested.clear()

    self._workers = [ ]
    for i in range(self._num_workers):
      self._workers.append(self._NewWorkerThread(i))

    # indicate that the thread pool has been started, so we can start
    # accepting connections
    self._running.set()

  def _NewWorkerThread(self, index):
    """
    Instantiates and starts a new worker thread.  Returns the thread
    object.

    This is called from Start().  It is split into its own method so
    that it can be overridden by subclasses.
    """
    worker = _ThreadPoolWorker(self._proxy, index, self._sleep_time)
    worker.start()
    return worker

  def Submit(self, op, args=(), name=None, queue=None):
    """
    Submits an operation to the queue.
    
    If the op argument is an instance of a subclass of ThreadPoolOperation,
    it will be queued up as-is.  In this case, the args, name, and queue
    arguments are ignored.

    Otherwise, op will be treated as a callable object that should be
    invoked to perform the operation, and it will be wrapped in an
    operation object and submitted to the queue.  The args argument
    is an optional list or tuple containing arguments to be passed to
    the callable object.  The name argument is an optional name for the
    operation.  The queue argument is an optional Queue.Queue object on
    which the operation object will be placed when the operation has
    been performed.

    Returns the submitted operation object on success.  Raises a
    ThreadPoolError exception if the pool is not running.
    """
    if not self._running.isSet():
      raise ThreadPoolError('thread pool not running')

    # if it's not a ThreadPoolOperation, wrap it in a _FunctionOperation
    # object
    if not isinstance(op, ThreadPoolOperation):
      op = _FunctionOperation(op, args=args, name=name, queue=queue)

    self._num_pending_ops.Increment()
    self._dispatch_queue.put(op)
    return op

  def NumPendingOperations(self):
    """
    Returns the number of pending operations.  An operation is pending
    if it has been passed to Submit(), but has not yet been performed by
    a worker thread.
    """
    return self._num_pending_ops.Value()

  def NumFinishedOperations(self):
    """
    Returns the number of finished operations.  An operation is finished
    if it has been performed by a worker thread.
    """
    return self._num_finished_ops.Value()

  def Shutdown(self, skip_pending_ops=0, wait_for_workers=1):
    """
    Terminates all worker threads.

    Note that worker threads will not be terminated immediately.  All
    operations that have already been queued up will be completed.
    However, no new operations will be allowed into the queue.

    If skip_pending_ops is true, operations that are already in the
    queue, but which have not yet been started by a worker thread, will
    be skipped.  Worker threads will terminate as soon as they complete
    the operation they are currently performing.  This is false by
    default.

    If wait_for_workers is true, this method will not return until all
    of the worker threads have terminated.  This is true by default.

    Note that this will not harm any operations that are already in the
    queue.  After the pool has been shut down, it can be re-started by
    calling Start() again.

    Raises a ThreadPoolError exception if the pool is not running.
    """
    if not self._running.isSet():
      raise ThreadPoolError('thread pool not running')

    logging.debug('shutting down thread pool...')

    # stop accepting new operations
    self._running.clear()

    # wait until the queue is empty
    if not skip_pending_ops:
      while not self._dispatch_queue.empty():
        logging.debug('dispatch queue not empty - sleeping for %f secs',
                      self._sleep_time)
        time.sleep(self._sleep_time)

    # tell the worker threads to exit when they finish their current op
    self._termination_requested.set()

    # wait for threads to terminate
    if wait_for_workers:
      for worker in self._workers:
        worker.join()
    self._workers = [ ]

  def WaitAll(self):
    """
    Wait until there are no more pending operations.

    Raises a ThreadPoolError exception if the pool is not running.
    """
    if not self._running.isSet():
      raise ThreadPoolError('thread pool not running')

    while self._num_pending_ops.Value():
      logging.debug('pending operations - sleeping for %f secs',
                    self._sleep_time)
      time.sleep(self._sleep_time)


###############################################################################
###  class _ThreadPoolProxy
###############################################################################

class _ThreadPoolProxy:
  """
  _ThreadPoolProxy contains methods which would ideally be part of
  _ThreadPool, but are implemented in a separate class to avoid
  circular references (the pool pointing to workers and back). The
  cycle detector and weak references could take care of this problem,
  but not fast enough, as threads are a very scare resource.
  """

  def __init__(self, num_pending, num_finished,
               termination_requested, queue):
    self._num_pending_ops = num_pending
    self._num_finished_ops = num_finished
    self._termination_requested = termination_requested
    self._dispatch_queue = queue

  def _DoneWithOperation(self, op):
    """
    For use by _ThreadPoolWorker class.  Indicates that the given
    operation has been completed.
    """
    self._num_pending_ops.Decrement()
    self._num_finished_ops.Increment()
    queue = op._Queue()
    if queue is not None:
      queue.put(op)

  def _GetNextOperation(self):
    """
    For use by _ThreadPoolWorker class.  Gets the next operation to be
    performed.  Raises a Queue.Empty exception if there are no
    operations waiting to be performed.
    """
    return self._dispatch_queue.get(0)

  def _TerminationRequested(self):
    """
    For use by _ThreadPoolWorker class.  Returns true when the worker
    threads should terminate; false otherwise.
    """
    return self._termination_requested.isSet()
