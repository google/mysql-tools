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
"""Some utilities for working with table ranges."""

import db
import schema

class Error(Exception):  pass
class NoPrimaryKey(Error):  pass


class PrimaryKeyRange(object):
  """A class for operations on a MySQL table based on PRIMARY KEY ranges.

  This class provides operations on MySQL rows by PRIMARY KEY ranges.
  Find the last row of a range, the first row of a range, the nth row of a
  a range and all rows that are within a primary key range.

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
    return ', '.join('`%s`' % column for column in self._table.GetPrimaryKey())

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
      Returns None if no records found in table.
    """
    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['optional_where'] = optional_where
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(optional_where)s LIMIT 1')
    res = self._dbh.ExecuteOrDie(sql % attrs)
    if res:
      return res[0]

  def GetLastRow(self):
    """Get the last row of a table (in PRIMARY KEY order).

    Returns:
      A dict of the PRIMARY KEY columns and their values.
      Returns None if no records found in table.
    """
    # We need the following sql (assuming a PRIMARY KEY (a,b,c)):
    # ORDER BY a DESC, b DESC, c DESC
    order_desc = []
    for c in self._table.GetPrimaryKey():
      order_desc.append('`%s` DESC' % c)

    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['order_by'] = ', '.join(order_desc)
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'ORDER BY %(order_by)s LIMIT 1')

    res = self._dbh.ExecuteOrDie(sql % attrs)
    if res:
      return res[0]

  def _GenerateInequalityWhere(self, key, operator, cols):
    predicate = []
    ret = []
    for col in cols:
      parts = []
      if predicate:
        parts.append(' AND '.join(predicate))
      parts.append('`%s` %s %s' % (
          col, operator, self._dbh.Escape(key[col])))
      ret.append(' AND '.join(parts))
      predicate.append('`%s` = %s' % (
          col, self._dbh.Escape(key[col])))
    return ' OR '.join(ret)

  def GetNthPrimaryKeyValue(self, n, initial_key=None):
    """Return the Nth PRIMARY KEY value after initial_key.

    If there aren't N elements in the table, return the last row of the table.

    Args:
      n: (int) The Nth element in the result set.
      initial_key: (dict) The PRIMARY KEY values for the start of our range.
        {col_name: value, ...}
    Returns:
      A dict of PRIMARY KEY values that identify the Nth value.
      Returns None if no records found in table.
    """
    assert n > 0, 'N must be > 0.'

    if not initial_key:
      initial_key = self.GetFirstPrimaryKeyValue()
    # If initial_key is None, empty table, return None.
    if not initial_key:
      return
    attrs = self._attrs.copy()
    attrs['select'] = self._PrimaryKeyForSelect()
    attrs['where'] = self._GenerateInequalityWhere(
        initial_key, '>', list(self._table.GetPrimaryKey()))
    attrs['n'] = n - 1

    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(where)s '
           'LIMIT %(n)d,1')
    res = self._dbh.ExecuteOrDie(sql % attrs)
    if not res:
      return self.GetLastRow()
    return res[0]

  def _GenerateRangePredicate(self, start_key, end_key, cols):
    """Generate an equality expression for common values between two keys.

    Args:
      start_key: (dict) The PRIMARY KEY values for the start of the range.
      end_key: (dict) The PRIMARY KEY values for the end of the range.
      cols: The ordered list of primary key fields. This is consumed up to the
        point where start_key and end_key differ, so it can be used after this
        call as a list of variant fields.

    Returns:
      String containing a SQL expression for the equal fields.
    """
    predicate = []
    for col in list(cols):
      if start_key[col] != end_key[col]:
        break
      cols.pop(0)
      predicate.append('`%s` = %s' % (
          col, self._dbh.Escape(start_key[col])))
    return ' AND '.join(predicate) or '1'

  def _GenerateRangeSide(self, key, operator, cols):
    """Generate an expression for all the values in a side tree.

    Args:
      key: (dict) The PRIMARY KEY values for the start or end of the range.
      operator: '>' if key is the start of a range and this is the left side;
        '<' if key is the end of a range and this is the right side.
      cols: The columns that vary between start_key and end_key.

    Returns:
      String containing a SQL expression for the side tree.
    """
    if not cols:
      return '1'
    if len(cols) == 1:
      col = cols[0]
      return '`%s` = %s' % (
          col, self._dbh.Escape(key[col]))
    else:
      col1 = cols[0]
      col2 = cols[1]
      return '(`%s` = %s AND (`%s` %s %s OR %s))' % (
          col1, self._dbh.Escape(key[col1]),
          col2, operator, self._dbh.Escape(key[col2]),
          self._GenerateRangeSide(key, operator, cols[1:]))

  def _GenerateRangeCenter(self, start_key, end_key, cols):
    """Generate an inequality expression for all the values between two trees.

    Args:
      start_key: (dict) The PRIMARY KEY values for the start of the range.
      end_key: (dict) The PRIMARY KEY values for the end of the range.
      cols: The columns that vary between start_key and end_key.

    Returns:
      String containing a SQL expression for the center rows.
    """
    if not cols:
      return '1'
    col = cols[0]
    return '(`%s` > %s AND `%s` < %s)' % (
        col, self._dbh.Escape(start_key[col]),
        col, self._dbh.Escape(end_key[col]))

  def GenerateRangeWhere(self, start_key, end_key):
    """Given a start and end, generate a where clause to select a range.

    Return a where clause that SELECTs all rows between start_key and end_key.
    This is somewhat tricky for multiple column PRIMARY KEYs since the
    WHERE clause depends on which columns between the two keys differ.

    The expression below first generates an equality match for all columns in a
    continuous path from the beginning of the key that are the same, i.e. the
    common parent tree between start and end keys.

    For the first column that varies, it then generates three sets and takes the
    union of them: the "left" tree under the starting value, the set of rows
    between the start and end values, and the "right" tree under the ending
    value.

    Args:
      start_key: (dict) PRIMARY KEY value that identifies the first row.
      end_key: (dict) PRIMARY KEY value that identifies the last row.
    Returns:
      A string WHERE clause for the appropriate Range select.
    """
    cols = list(self._table.GetPrimaryKey())
    return ''.join((
        '(',
        self._GenerateRangePredicate(start_key, end_key, cols),
        ' AND (',
        self._GenerateRangeSide(start_key, '>', cols),
        ' OR ',
        self._GenerateRangeCenter(start_key, end_key, cols),
        ' OR ',
        self._GenerateRangeSide(end_key, '<', cols),
        '))',
    ))

  def RangePrimaryKeyValues(self, start_key, end_key, select=None):
    """Return rows between start_key (inclusive) and end_key (inclusive).

    Args:
      start_key: (dict) PRIMARY KEY value that identifies the first row.
      end_key: (dict) PRIMARY KEY value that identifies the last row.
      select: (str) The values you want to select from this table.  If None,
              it defaults to the columns that make up the primary key.
    Returns:
      A list of row objects (dicts).
      Returns None if no records found in table.
    """
    attrs = self._attrs.copy()
    attrs['where'] = self.GenerateRangeWhere(start_key, end_key)
    attrs['select'] = select or self._PrimaryKeyForSelect()
    sql = ('SELECT %(select)s '
           'FROM %(db)s.%(table)s FORCE INDEX (PRIMARY) '
           'WHERE %(where)s')
    res = self._dbh.ExecuteOrDie(sql % attrs)
    if res:
      return res
