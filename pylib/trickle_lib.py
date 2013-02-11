# Copyright 2006 Google Inc. All Rights Reserved.
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

"""A library to allow for long running, slow operations.

Intended mainly for database operations, this library provides a
skeleton to safely execute a given set of operations such that the
runtime is a fraction of realtime (such as N% utilization).  It is
designed to keep track of its position and be restartable, though that
is dependent upon the class that subclasses TrickledOperation.
"""

__author__ = 'bbiskebo@google.com (Brian Biskeborn)'
# Original author: chip@google.com (Chip Turner)

import logging
import time


class TrickledOperation(object):
  """The class representing both how a trickle operation is performed and
  the state associated with a given operation.

  Users of this class will subclass it and implement the protected
  methods _SetupTrickle, _Finished, _PerformTrickle, and
  _FinalizeTrickle.  These methods will be called during a trickle
  operation such that the size of the batch passed to _PerformTrickle
  tries to be exactly utilization_percent of each cycle_time.

  Specifying utilization_percent=100 turns off all sleeping, and
  does the work at maximum speed.
  """
  def __init__(self, utilization_percent, cycle_time):
    """Constructor.  Subclass and store operation-specific state here.

    Args:
      utilization_percent: percent of time to spend in _PerformTrickle
      cycle_time: interval over which utilization_percent is
                  calculated per run
    """
    self._utilization_fraction = utilization_percent / 100.0
    self._cycle_time = cycle_time
    self._batch_size = 1    # Start maximally cautious (single statement)
                            # and increase batch size as we can
    self._batch_size_limit = 1000000
    self.verbose = True

  def _SetupTrickle(self):
    """A setup method, invoked before a trickle loop is run.

    It is valid to execute multiple loops, and this method will be
    invoked for each.  Useful for finding the next starting position.
    """
    raise NotImplementedError('call to pure virtual function')

  def _FinalizeTrickle(self):
    """A completion method copied after a series of trickle loops.

    Intended to finish up state, such as the final 'slosh' rows of a
    continual copy loop.
    """
    raise NotImplementedError('call to pure virtual function')

  def _Finished(self):
    """Called to determine if a trickle is complete."""
    raise NotImplementedError('call to pure virtual function')

  def _PerformTrickle(self, batch_size):
    """The method invoked to perform the actual trickle operation.

    This method will be invoked multiple times and is passed the size
    of a batch to execute, which will vary depending upon the runtime
    of the previous batch.

    Args:
      batch_size: size of the current batch (subclass dependent)

    Returns:
      number of items processed (usually batch_size)
    """
    raise NotImplementedError('call to pure virtual function')

  def _GetProgress(self):
    """Called to fetch progress information

    This method is intended to be overridden by inheriting classes
    that can provide data about their progress through the trickle.

    Args:
      none
    Returns:
      String representation of current progress state or None if unknown
    """
    return None

  def SetBatchSizeLimit(self, n):
    """Set the maximum batch_size that will ever be submitted by Trickle().

    Intended for operations that need to be artificially slowed down in
    excess of what the throttler would have done.  For instance, when
    replicas are drastically slower than the primary for some reason, which
    the throttler will be unable to detect.

    Args:
      n: How many queries may be run in a batch. Values above 1000000 are
          probably ill-advised.
    """
    self._batch_size_limit = n

  def Trickle(self):
    """Perform the actual trickle operation.

    This function will loop until self._Finished() returns true.  It
    calls the above methods in this order:

    self._SetupTrickle()
    while not self._Finished():
      ...
      self._PerformTrickle(self, batch_size)
      ...
    self._FinalizeTrickle()

    Args:
      None
    Returns:
      None
    """
    self._SetupTrickle()

    # track the last ten cycles worth of copy rates
    copy_rates = [0] * 10
    copy_rate_idx = 0

    # also track average copies over this invocation's lifetime
    rows_copied = 0
    start_time = time.time()

    while not self._Finished():
      then = time.time()
      batch_size = self._batch_size

      rowcount = self._PerformTrickle(batch_size)

      # Increase or decrease batch size to get the target utilization
      # percentage at the given cycle time. At all times, we're cautious about
      # floating point rounding effects and the like.
      time_delta = time.time() - then
      ideal_delta = self._cycle_time * self._utilization_fraction

      if time_delta > 2.0 * ideal_delta:
        # If our utilization fraction is way too high, we want to decrease our
        # batch size fairly drastically, but we don't want to chop things to
        # nothing in case what we saw was a transient blip. Note that if we get
        # here, either (a) we've seen enough short statements to increase our
        # batch size from its initial value of 1 before hitting this, or (b) a
        # batch size of 1 is still way too big, in which case we modulate cycle
        # time below instead.
        self._batch_size = max(int(batch_size/2), 1)
      elif time_delta * 1.5 < ideal_delta:
        # We don't want to increase batch size quite as drastically
        self._batch_size = max(int(batch_size * 1.5), batch_size + 1)
      else:
        # batch_size is between 2x and 0.67x target. Modulate it directly to
        # the target value.
        self._batch_size = max(int(batch_size * ideal_delta / time_delta), 1)

      # Rev limiter in case an operation is a no-op by accident.
      self._batch_size = min(self._batch_size, self._batch_size_limit)

      # How long are we going to sleep this time? At least the rest of the
      # cycle time, longer if utilization would otherwise be too high, and
      # at least one second regardless but no more than 2x the cycle time.
      if self._utilization_fraction < 1.0:
        sleep_time = min(
            max(self._cycle_time - time_delta,
                (time_delta / self._utilization_fraction) - time_delta,
                1),
            2 * self._cycle_time)
      else:
        # But if running with 100% utilization, don't sleep at all.
        sleep_time = 0.0

      # update average copy rate
      this_batch_rate = batch_size / (sleep_time + time_delta)
      copy_rates[copy_rate_idx] = this_batch_rate
      copy_rate_idx = (copy_rate_idx + 1) % len(copy_rates)
      current_rate_avg = sum(copy_rates) / float(len(copy_rates))
      rows_copied += rowcount

      if self.verbose:
        self._LogStatus(batch_size, start_time, time_delta, sleep_time,
                        current_rate_avg, rows_copied, copy_rate_idx)

      time.sleep(sleep_time)

    if self.verbose:
      self._LogFinish(rows_copied, start_time)
    self._FinalizeTrickle()

  def _LogStatus(self, batch_size, start_time, time_delta, sleep_time,
                 current_rate_avg, rows_copied, copy_rate_idx):
    progress = self._GetProgress()
    if progress:
      progress = ', ' + progress
    else:
      progress = ''

    logging.info('batch of %d in %.2f s%s, sleeping %.2f s'
                 % (batch_size, time_delta, progress, sleep_time))
    logging.info('util %.2f, new batch size %d '
                 '(%.2f current, %.2f avg rows/sec)'
                 % (time_delta / (time_delta + sleep_time),
                    self._batch_size,
                    current_rate_avg,
                    rows_copied / (time.time() - start_time)))

  def _LogFinish(self, rows_copied, start_time):
    logging.info('Done: %.2f avg rows/sec',
                 (rows_copied / (time.time() - start_time)))


class GeneratorOperation(TrickledOperation):
  """Adapts blocking functions so they can run within trickle_lib.

  The adapter only requires that users insert a 'yield' statement after each
  entry in the batch has been processed.
  """

  def __init__(self, generator, utilization_percent, cycle_time):
    """Constructor.

    Args:
      generator: A function, that does work every time every time it is iterated
          over.
      utilization_percent: An int, percent of time to spend in _PerformTrickle.
      cycle_time: interval over which utilization_percent is
                  calculated per run
    """
    TrickledOperation.__init__(self, utilization_percent, cycle_time)
    self._generator = generator
    self._finished = False

  def _SetupTrickle(self):
    pass

  def _FinalizeTrickle(self):
    pass

  def _Finished(self):
    return self._finished

  def _PerformTrickle(self, batch_size):
    processed = 0
    try:
      for _ in xrange(batch_size):
        self._generator.next()
        processed += 1
    except StopIteration:
      self._finished = True

    return processed
