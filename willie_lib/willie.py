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

import time

class Willie(object):
  """Groundskeeper willie class."""

  def __init__(self, dbh, table, condition, limit,
               utilization_percent, dry_run):
    """
      Args:
        dbh: The database handle.
        table: The table to cleanup.
        condition: The conditional to get results for.
        limit: The biggest batch size to get.
        utilization percent: The utilization percentage.
        dry_run: Do this for real or not.
    """
    self._dbh = dbh
    self._table = table
    self._condition = condition
    self._limit = limit
    self._utilization_percent = utilization_percent
    self._dry_run = dry_run
    self._primary_key_column = self.GetPrimaryKey()

  def GetPrimaryKey(self):
    """Gets the Primary Key For a Table.

    Returns:
      Returns the primary key for a table.
    """
    indexes = self._dbh.ExecuteOrDie("SELECT COLUMN_NAME FROM "
                                     "INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                                     "WHERE TABLE_NAME='%s' AND "
                                     "CONSTRAINT_NAME='PRIMARY';"
                                     % (self._table))
    assert len(indexes) == 1
    return indexes[0]['COLUMN_NAME']

  def FetchRows(self):
    """Fetchs rows to delete from the database.

    Returns:
      Returns results from the database to operate on.
    """
    if self._limit:
      rows = self._dbh.ExecuteOrDie("SELECT %s FROM %s WHERE "
                                    "%s LIMIT %s;"
                                    % (self._primary_key_column, self._table,
                                       self._condition, self._limit))
    else:
      rows = self._dbh.ExecuteOrDie("SELECT %s FROM %s WHERE %s;"
                                    % (self._primary_key_column, self._table,
                                       self._condition))
    values = [row['RequestId'] for row in rows]
    return values

  def PerformDelete(self, results):
    """Deletes results from the database."""
    start_time = time.time()
    result = ','.join(str(x) for x in results)
    delete_statement = ("DELETE FROM %s WHERE %s IN (%s)"
                        % (self._table, self._primary_key_column, result))
    print  "Delete statement: %s" % delete_statement
    if not self._dry_run:
      self._dbh.ExecuteOrDie(delete_statement)
      end_time = time.time()
      delta_time = end_time - start_time
      delay_time = delta_time * (100.0 / self._utilization_percent) - delta_time
      print  "Delay Time: %s" % delay_time
      time.sleep(delay_time)


  def Loop(self):
    while True:
      keys = self.FetchRows()
      if keys:
        self.PerformDelete(keys)
        if self._dry_run:
          return
      else:
        return
