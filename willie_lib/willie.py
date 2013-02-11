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

"""A library for slowly deleting data from a table."""

import csv
import time

try:
  from ..pylib import range_lib
except (ImportError, ValueError):
  from pylib import range_lib


class CsvWriter(object):
  """Writes the changes out to csv."""

  def __init__(self, filename):
    self._filename = filename
    self._writer = None

  def WriteRows(self, rows):
    listkeys = rows[0].keys()
    if not self._writer:
      self._writer = csv.DictWriter(open(self._filename, 'wb'), listkeys)
    self._writer.writerows(rows)


writer_method = {'CSV': CsvWriter}


class Willie(object):
  """Groundskeeper willie class."""

  def __init__(self, dbh, database_name, table, condition, limit,
               utilization_percent, dry_run, writer_type, filename):
    """Constructor.

    Args:
      dbh: The database handle.
      database_name: The Database Name.
      table: The table to cleanup.
      condition: The conditional to get results for.
      limit: The biggest batch size to get.
      utilization_percent: The utilization percentage.
      dry_run: Do this for real or not.
      writer_type: Type of file to write out delete rows.
      filename: Name of file to write out delete rows to.
    """
    self._dbh = dbh
    self._database_name = database_name
    self._table = table
    self._condition = condition
    self._limit = limit
    self._utilization_percent = utilization_percent
    self._dry_run = dry_run
    self._writer_type = writer_type
    self._filename = filename
    if not dry_run and writer_type:
      self._writer = writer_method[writer_type](filename)
    else:
      self._writer = None

  def GetRange(self):
    """Gets ranges to delete using range_lib based on a set condition."""
    rangeh = range_lib.PrimaryKeyRange(self._dbh, self._database_name,
                                       self._table)
    first_key = rangeh.GetFirstPrimaryKeyValue(self._condition)
    nth_key = rangeh.GetNthPrimaryKeyValue(self._limit, first_key)
    if self._writer:
      rows = rangeh.RangePrimaryKeyValues(first_key, nth_key, '*')
      self._writer.WriteRows(rows)
    return rangeh.GenerateRangeWhere(first_key, nth_key)

  def PerformDelete(self, where_clause):
    """Deletes results from the database."""
    start_time = time.time()
    delete_statement = ('DELETE FROM %s.%s WHERE %s'
                        % (self._database_name, self._table, where_clause))
    print 'Delete statement: %s' % delete_statement
    if not self._dry_run:
      self._dbh.ExecuteOrDie(delete_statement)
      end_time = time.time()
      delta_time = end_time - start_time
      delay_time = delta_time * (100.0 / self._utilization_percent) - delta_time
      print 'Delay Time: %s' % delay_time
      time.sleep(delay_time)

  def Loop(self):
    while True:
      where_clause = self.GetRange()
      if where_clause:
        self.PerformDelete(where_clause)
        if self._dry_run:
          return
      else:
        return
