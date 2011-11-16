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


import db
import schema

class Error(Exception):  pass
class NoPrimaryKey(Error):  pass


class PrimaryKeyRange(object):
  """A class for operations on a MySQL table based on PRIMARY KEY ranges.

  This class provides operations on MySQL rows by PRIMARY KEY ranges.
  Find the last row of a range, the first row of a range, the nth row of a
  a range and all rows that are  within a primary key range.

  Typical usage:
    t = PrimaryKeyRange(dbh, 'db', 'table')
    first_key = t.GetFirstPrimaryKeyValue()
    nth_key = t.GetNthPrimaryKeyValue(n, first_key)
    range = t.RangePrimaryKeyValues(first_key, nth_key)
  """

  def __init__(self, dbh, table_schema, table_name):
    """
    Args:
      dbh: a db.py connection handle.
      table_schema: (str) the MySQL database name.
      table_name: (str) the MySQL table name.
    """
    self._dbh = dbh
    self.table_schema = table_schema # Name of the db table resides in.
    self.table_name = table_name # Name of the table.
    self._attrs = {}
    self._attrs['db'] = self.table_schema
    self._attrs['table'] = self.table_name
    db = schema.Database(dbh, self.table_schema)
    self._table = schema.Table(dbh, db, self.table_name)
    if not self._table.GetPrimaryKey():
      raise NoPrimaryKey('Table: %s does not have a primary key.'
                          % self.table_name)

  def _PrimaryKeyForSelect(self):
    """Format primary key column list for use in a SELECT statement."""
    return ', '.join('CONVERT(%s USING binary) AS %s' % (column, column)
                     for column in self._table.GetPrimaryKey())

  def _PrimaryKeyForWhere(self):
    """Format primary key column list for use in a WHERE clause."""
    return ', '.join(self._table.GetPrimaryKey())

  def GetFirstPrimaryKeyValue(self, optional_where='1'):
    """Return the PRIMARY KEY for the first row of the table.

    Return the PRIMARY KEY row value for the first row in the table if
    optional_where is undefined.

    If optional_where is specified, return the first PRIMARY KEY row value
    from the corresponding result set.

    Args:
      optional_where: (str) if unspecified returns the first row of the table.
        If specified, return the first PRIMARY KEY row within the result set.
    Returns:
      The PRIMARY KEY values for the start of our range.
        {col_name: value, ...}
    """
    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['optional_where'] = optional_where
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(optional_where)s LIMIT 1')
    res = self._dbh.ExecuteOrDie(sql % attrs)
    return res[0]

  def _MakeKeyTuple(self, key):
    """Given a key, return a string of that key in tuple format

    Use the known order of the primary key to format the key.

    Args:
      key: (dict) PRIMARY KEY value
    Returns:
      A string of the form '("c1", "c2", "cn", ...)'
    """
    key_tuple = ('_binary%s' % self._dbh.Escape(key[col])
                 for col in self._table.GetPrimaryKey())
    return '(%s)' % (', '.join(key_tuple))

  def _GenerateNthWhere(self, start_key):
    """Method to generate the where clause to find the Nth Row.

    E.g. We have a 3 column primary key (a,b,c) we must generate a WHERE like:
      (a,b,c) > (start_a, start_b, start_c)

    Args:
      start_key: the key we want our rows to be greater than.
    Returns:
      A string WHERE clause.
    """
    return "(%s) > %s" % (self._PrimaryKeyForWhere(),
                          self._MakeKeyTuple(start_key))

  def GetLastRow(self):
    """Get the last row of a table (in PRIMARY KEY order).

    Returns:
      A dict of the PRIMARY KEY columns and their values.
    """
    # We need the following sql (assuming a PRIMARY KEY (a,b,c)):
    # ORDER BY a DESC, b DESC, c DESC
    order_desc = []
    for c in self._table.GetPrimaryKey():
      order_desc.append('%s DESC ' % c)

    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['order_by'] = ','.join(order_desc)
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'ORDER BY %(order_by)s LIMIT 1')

    return self._dbh.ExecuteOrDie(sql % attrs)[0]

  def GetNthPrimaryKeyValue(self, n, initial_key=None):
    """Return the Nth PRIMARY KEY value after initial_key.

    If there aren't N elements in the table, return the last row of the table.

    Args:
      n: (int) The Nth element in the result set.
      initial_key: (dict) The PRIMARY KEY values for the start of our range.
        {col_name: value, ...}
    Returns:
      A dict of PRIMARY KEY values that identify the Nth value.
    """
    assert n > 0, 'N must be > 0.'

    n = n - 1
    if not initial_key:
      initial_key = self.GetFirstPrimaryKeyValue()
    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['where'] = self._GenerateNthWhere(initial_key)
    attrs['n'] = n

    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(where)s '
           'LIMIT %(n)d,1')
    res = self._dbh.ExecuteOrDie(sql % attrs)
    # If we don't get a result back, it's because we've tried to SELECT
    # past the end of the table. Return the PRIMARY KEY for the last row.
    if not res:
      return self.GetLastRow()
    return res[0]

  def GenerateRangeWhere(self, start_key, end_key):
    """Given a start and end, generate a where clause to select a range.

    Return a where clause that SELECTs all rows between start_key and end_key.
    This is somewhat tricky for multiple column PRIMARY KEYs since the
    WHERE clause depends on which columns between the two keys differ.

    Args:
      start_key: (dict) PRIMARY KEY value that identifies the first row.
      end_key: (dict) PRIMARY KEY value that identifies the last row.
    Returns:
      A string WHERE clause for the appropriate Range select.
    """
    start_tuple = self._MakeKeyTuple(start_key)
    end_tuple = self._MakeKeyTuple(end_key)

    where = []
    where.append("(%s) >= %s" % (self._PrimaryKeyForWhere(), start_tuple))
    where.append("(%s) <= %s" % (self._PrimaryKeyForWhere(), end_tuple))

    return ' AND '.join(where)

  def RangePrimaryKeyValues(self, start_key, end_key, select=None):
    """Return rows between start_key (inclusive) and end_key (inclusive).

    Args:
      start_key: (dict) PRIMARY KEY value that identifies the first row.
      end_key: (dict) PRIMARY KEY value that identifies the last row.
      select: (str) The values you want to select from this table.  If None,
              it defaults to the columns that make up the primary key.
    Returns:
      A list of row objects (dicts).
    """
    attrs = self._attrs.copy()
    attrs['where'] = self.GenerateRangeWhere(start_key, end_key)
    attrs['select'] = select or self._PrimaryKeyForSelect()
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(where)s')
    return self._dbh.ExecuteOrDie(sql % attrs)
