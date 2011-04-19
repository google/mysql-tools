#!/usr/bin/python2.6
#
# Copyright 2007-2011 Google Inc.
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

"""Scheduling policies for online data drift checking.

This file contains the three helper classed required by a DbChecksummer:
ProgressMonitor: Tracks performance and progress of checksum job.
BatchSizePolicy: Determines rows to scan in a single query
WaitTimePolicy: Suggests time to sleep between queries.

Original author: Ben Handy
Later maintainer: Mikey Dickerson
"""


class ProgressMonitor(object):
  """Monitors checksum job progress.

  This class is used by the rate limiting policies to determine the number
  of rows to scan in a query, or the amount of time to sleep between queries.
  It is also used to provide status information on the performance and progress
  of a checksum job.
  """

  def __init__(self, scan_rate):
    """Initialize rate limiter with checksumming job parameters.

    Args:
      scan_rate: estimated scan rate in bytes/sec for checksumming queries.
    """
    # Initialize user-specified values
    self.scan_rate = scan_rate

    # Initialize computed values to 0
    self.rows_estimated = 0
    self.secs_required = 0.0
    self.data_estimated = 0
    self.table_dict = {}

    # Initialize variables for tracking progress
    self.rows_scanned = 0
    self.secs_scanned = 0.0
    self.data_scanned = 0
    self.queries_executed = 0
    self.secs_sleep = 0.0
    self.secs_last_query = 0.0

  def __getstate__(self):
    """Save stats about the queries that have already executed on this run.

    Estimates on the size of the entire run, and the size of each table will
    be regenerated.
    """
    state_dict = {}
    state_dict['scan_rate'] = self.scan_rate
    state_dict['rows_scanned'] = self.rows_scanned
    state_dict['secs_scanned'] = self.secs_scanned
    state_dict['data_scanned'] = self.data_scanned
    state_dict['queries_executed'] = self.queries_executed
    state_dict['secs_sleep'] = self.secs_sleep
    state_dict['secs_last_query'] = self.secs_last_query
    state_dict['rows_estimated'] = self.rows_estimated
    state_dict['data_estimated'] = self.data_estimated
    state_dict['secs_required'] = self.secs_required
    state_dict['table_dict'] = self.table_dict
    return state_dict

  def AddTable(self, name, rows, data_size):
    """Add table information and data size estimates for a checksumming run."""
    self.rows_estimated += rows
    self.data_estimated += data_size
    self.secs_required += (data_size / self.scan_rate)
    self.table_dict[name] = (rows, data_size)

  def RecordProgress(self, rows, run_time, table):
    """Allows checksummer to communicate progress to this scheduler class."""
    self.secs_last_query = run_time
    self.rows_scanned += rows
    self.secs_scanned += run_time
    self.queries_executed += 1
    if self.table_dict.get(table, None):
      table_rows, table_data = self.table_dict[table]
      if table_rows and table_data:
        self.data_scanned += (rows * (table_data / table_rows))

  def ReportPerformance(self):
    """Provide current progress and performance details.

    Returns:
      A string containing performance and progress information.
    """
    if self.secs_scanned:
      return ('queries: %s, rows: %s, expected: %s, k/sec: %f\t rows/sec: '
              '%f query secs: %s, wait secs: '
              '%s, utilization: %s' %
              (self.queries_executed, self.rows_scanned,
               self.rows_estimated,
               self.data_scanned / (1000*self.secs_scanned),
               self.rows_scanned / self.secs_scanned,
               self.secs_scanned, self.secs_sleep,
               self.secs_scanned / (self.secs_scanned +
                                    self.secs_sleep)))


class BatchSizePolicy(object):
  """This defines the interface for providing row-per-query estimates.

  This class should not be instantiated directly.
  """

  def GetNextBatchSize(self, table_name):
    """Suggest next checksum query batch size in rows.

    Args:
      table_name: name of the table for the next checksum query
    """
    raise NotImplementedError('GetNextBatchSize() not implemented.')


class WaitTimePolicy(object):
  """This defines the interface for providing sleep-between-query estimates.

  This class should not be instantiated directly.
  """

  def GetNextWaitTime(self, table_name):
    """Suggest time to wait before issuing next checksum query."""
    raise NotImplementedError('GetNextWaitTime() not implemented.')


class FixedQuerySizePolicy(BatchSizePolicy):
  """This policy sets a fixed number of rows for all checksum queries.

  This is a BatchSizePolicy, providing the GetNextBatchSize function.
  """

  def __init__(self, monitor, batch_size):
    """Constructor.

    Args:
      monitor: instance of ProgressMonitor that is watching the checksum job.
      batch_size: Number of rows to scan in a single checksum query.
    """
    self._monitor = monitor
    self._batch_size = batch_size

  def GetNextBatchSize(self, unused_table_name):
    """Suggest next checksum query batch size in rows.

    Batch size is always the same, and is provided in the constructor.

    Returns:
      Number of rows to scan in next checksum query.
    """
    return self._batch_size


class FixedQueryTimePolicy(BatchSizePolicy):
  """This policy attempts to fix the time duration of all checksum queries.

  This is a BatchSizePolicy, providing the GetNextBatchSize function.
  """

  def __init__(self, monitor, query_duration):
    """Constructor.

    Args:
      monitor: instance of ProgressMonitor that is watching the checksum job.
      query_duration: desired duration of a single checksum query.
    """
    self._monitor = monitor
    self._query_duration = query_duration

  def GetNextBatchSize(self, table_name):
    """Suggest next checksum query batch size in rows.

    Batch size is based on avg row size for this table and desired duration.

    Args:
      table_name: name of the table for the next checksum query
    Returns:
      Number of rows to scan in next checksum query.
    """
    assert self._monitor.table_dict.get(table_name, None)
    # Estimate average row size
    rows, data = self._monitor.table_dict[table_name]
    if data and rows:
      avg_row_size = data / rows
    elif self._monitor.data_estimated and self._monitor.rows_estimated:
      avg_row_size = self._monitor.data_estimated / self._monitor.rows_estimated
    else:
      avg_row_size = 1000  # Pulled this out of a hat.

    if not avg_row_size:
      return 10000  # Fall back to fixed batch size

    # Use row size, query duration, and scan rate to determine batch size
    # rows = (secs) * (bytes / sec) / (bytes / row)
    batch_size = self._query_duration * self._monitor.scan_rate / avg_row_size
    return int(batch_size)


class FixedUtilizationPolicy(WaitTimePolicy):
  """This policy keeps a fixed load on the database during a checksum job.

  This is a WaitTimePolicy, providing the GetNextWaitTime function.
  """

  def __init__(self, monitor, utilization):
    """Constructor.

    Args:
      monitor: instance of ProgressMonitor that is watching the checksum job.
      utilization: ratio of time the server should be execute checksum queries.
    """
    self._monitor = monitor
    self._utilization = utilization

  def SetUtilization(self, new_utilization_rate):
    """Change the utilization of the checksum job, even mid-run.

    Utilization rates are expected in the range of 0.001 to 0.5.
    Other rates can be set, but probably won't be achievable.

    Args:
      new_utilization_rate: ratio of time to spend checksumming.
    """
    self._utilization = new_utilization_rate

  def GetNextWait(self, unused_table_name):
    """Suggest time to wait before issuing next checksum query.

    Next wait time is based on previous query time.

    Returns:
      Number of seconds to wait before next checksum query.
    """
    query_plus_sleep_secs = (self._monitor.secs_last_query / self._utilization)
    sleep_secs = query_plus_sleep_secs - self._monitor.secs_last_query
    self._monitor.secs_sleep += (sleep_secs)
    return sleep_secs


class FixedRunTimePolicy(WaitTimePolicy):
  """This policy tries to complete a checksum job in a fixed amount of time.

  This is a WaitTimePolicy, providing the GetNextWaitTime function.
  """

  def __init__(self, monitor, total_run_secs):
    """Constructor.

    Args:
      monitor: instance of ProgressMonitor that is watching the checksum job.
      total_run_secs: number of seconds before the job should be completed.
    """
    self._monitor = monitor
    self._total_run_secs = total_run_secs
    self._utilization = 0.0

  def SetUtilization(self, new_utilization_rate):
    """Change the utilization of the checksum job, even mid-run.

    Utilization rates are expected in the range of 0.001 to 0.5.
    Other rates can be set, but probably won't be achievable.

    Args:
      new_utilization_rate: ratio of time to spend checksumming.
    """
    self._utilization = new_utilization_rate

  def GetNextWait(self, unused_table_name):
    """Suggest time to wait before issuing next checksum query.

    Wait time between queries is always the same.

    Returns:
      Number of seconds to wait before next checksum query.
    """
    if not self._utilization:
      total_query_secs = self._monitor.data_estimated / self._monitor.scan_rate
      self._utilization = float(total_query_secs) / float(self._total_run_secs)
    sleep_time = ((self._monitor.secs_last_query / self._utilization) -
                  self._monitor.secs_last_query)
    return sleep_time
