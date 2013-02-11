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

"""A thread-safe interface to a sharded database.

Connect() parses a dbspec and returns a Connection or MultiConnection object.
The objects can both be instantiated directly with args that are passed through
to MySQLdb.connect.  The resulting objects have an Execute() method that
enforces that all shards return the same result, and a MultiExecute() method
that returns a dictionary of results by expanded host name.

Query results are returned as VirtualTable objects, which act like dictionary
cursor return values (lists of dictionaries), though they are stored more
efficiently internally.  Failures are returned as QueryErrors or QueryWarnings
objects, which also act like tables, but with error contents.

Use example:
import db
cb = db.Connect('dbhost{0..9}:dbuser:?:dbname#')
cb.Execute('UPDATE foo SET bar=5 WHERE id=10')
cb.Close()
....
cb.Execute('UPDATE foo SET bar=10 WHERE id=10')
cb.Close()

There is also a SQL syntax extension for selecting from specific shard(s):
cb.Execute('ON SHARD 3,5 SELECT foo FROM bar')
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import decimal
import getpass
import itertools
import logging
import os
import pprint
import Queue
import random
import re
import socket
import sys
import threading
import time
import traceback
import weakref

import MySQLdb
from MySQLdb import converters


class Error(Exception):
  pass


class ResolutionError(Error):
  """Resolution failed."""


class InconsistentResponses(Error):
  """Responses differ between shards."""


class InconsistentSchema(Error):
  """Result schema differs between shards."""


class QueryErrorsException(Error):
  """Query returned error(s)."""


class QueryWarningsException(Error):
  """Query returned warning(s)."""


class Timeout(Error):
  """Timed out."""


class LockLost(Error):
  """Lost lock while executing."""


class RetriesExceeded(Error):
  """Exceeded allowed retry attempts."""


class InvalidShard(Error):
  """Invalid shard number specified."""


class InputRemaining(Error):
  """Input remains after we expected to consume it all."""


class CannotReadPassword(Error):
  """We could not read the password file."""


class MultipleIteration(Error):
  """Iterating over a consumed RowIterator. Should you call Populate()?"""


class InvalidShardMix(Error):
  """Mix of different sharding schemes where not allowed."""


def GetDefaultConversions():
  """Return a copy of the default value conversion dict."""
  return converters.conversions.copy()


CONNECTIONS = set()
_COMMENT_RE = re.compile(r'^\s*--\s.*')


class Spec(dict):
  """Represent a database specification.

  This is a dict of dbargs which can be passed to Connection or MultiConnection.
  """

  _DBSPEC_RE_TEMPLATE = r"""
      ^
      (?:(?P<dbtype>%s):)?      # Optional dbtype.
      (?P<host>[^:]+):          # Hostname.
      (?P<user>[^:]*):          # Username.
      (?P<password>[^:]*)       # Password.
      (?::(?P<db>[^:]*))?       # Optional dbname.
      (?::(?P<port>\d+))?       # Optional port.
      $
      """

  # Valid DB types (first part of a 5-part dbspec)
  _DB_TYPES = [
      'mysql'
  ]

  _DEFAULT_DB_TYPE = 'mysql'

  # (user, host) -> password
  _PW_CACHE = {}

  @classmethod
  def _GetDbSpecRe(cls):
    """Interpolates _DBSPEC_RE_TEMPLATE with self._DB_TYPES.

    Returns:
      Regular expression.
    """
    return re.compile(
        cls._DBSPEC_RE_TEMPLATE % ('|'.join(map(re.escape, cls._DB_TYPES))),
        re.VERBOSE)

  def _FetchPassword(self):
    """Evaluates pfile= in self['passwd']'.

    It can also modify some other fields, like the username.

    Raises:
      CannotReadPassword: As the exception name indicates.
    """
    try:
      if self['passwd'].startswith('pfile='):
        self['passwd'] = open(self['passwd'][6:]).read().strip()
    except IOError as e:
      raise CannotReadPassword(e)

  @classmethod
  def Parse(cls, spec, **kwargs):
    """Parse the spec string into a dict of dbargs.

    Several types of dbspec hostnames are supported:
      dbhost3: A single DNS host
      dbhost#: Shard count read from ConfigurationGlobals.NumShards on shard 0
      dbhost0,dbhost1: List of hosts
      dbhost{0..9}: A range of shards that match a DNS name
      socket=/var/run/mysql/mysqld.sock: UNIX socket

    The database part of the dbspec can be:
      dbname3: A single database
      dbname#: Shard substitution from the hostname list, or from NumShards
      dbname0,dbname1: List of databases

      Mixing the list and # forms will produce undefined results.

    Args:
      spec: The dbspec to connect to (see above)
      **kwargs: Additional arguments are passed to Spec.__init__.  Note that
        these override the parsed contents of 'spec'.
    Returns:
      a Spec object (a dictionary of connect parameters)
    Raises:
      ValueError: if spec is invalid
    """

    pattern = cls._GetDbSpecRe()
    match = pattern.search(spec)
    if not match:
      raise ValueError('Invalid DBSpec "%s", does not match re:\n%s'
                       % (spec, pattern.pattern))
    args = match.groupdict()

    if args['dbtype']:
      kwargs.setdefault('dbtype', args['dbtype'])
    if args.get('port'):
      kwargs.setdefault('port', int(args['port']))
    kwargs.setdefault('host', args['host'])

    if not kwargs.setdefault('user', args['user']):
      kwargs['user'] = os.getenv('USER')
    if kwargs.setdefault('db', args['db']) is None:
      kwargs['db'] = ''

    if ((not args['password'] and sys.stdin.isatty())
        or args['password'] == '?'):
      userhost = (kwargs['user'], kwargs['host'])
      if userhost not in cls._PW_CACHE:
        cls._PW_CACHE[userhost] = getpass.getpass(
            'Password for %s@%s: ' % userhost)
      args['password'] = cls._PW_CACHE[userhost]

    kwargs.setdefault('passwd', args['password'])
    return cls(**kwargs)

  def __init__(self, **args):
    """Construct a new Spec using args.

    Args:
      **args:  Args to pass to the connection_class in Connect().  Examples:
        'dbtype' - Must be in _DB_TYPES (optional, defaults to mysql)
        'host' - A hostname specification, see db.Spec.Parse().
        'user' - DB user name
        'passwd' - DB user's password
        'db' - The database to open.  Defaults to ''.
        'port' - Port number to connect to at the host. (optional)
        'charset' - Character set for both directions of the connection.
          (optional, defaults to utf8)
        'conv' - Dictionary of python type or MySQL type constant to conversion
          function (optional, defaults to GetDefaultConversions())
    """
    args.setdefault('db', '')
    args.setdefault('dbtype', self._DEFAULT_DB_TYPE)
    assert args['dbtype'] in self._DB_TYPES, (
        'Unsupported dbtype %s' % args['dbtype'])
    args.setdefault('charset', 'utf8')
    args.setdefault('connect_timeout', 10)

    dict.__init__(self, args)

    # Handle UNIX socket host syntax
    if self['host'].startswith('socket='):
      self['unix_socket'] = self['host'][7:]
      self['host'] = 'localhost'

    # Some existing code relies on Spec to evaluate password files. This enables
    # that code to work.
    # TODO(unbrice): Remove this.
    if self['passwd'].startswith('pfile='):
      self._FetchPassword()

    self._expander = _GetExpander(self['host'], self)

  def __str__(self):
    """Returns the dbspec string for this instance, sans password."""
    result = '%s:%s:%s:*:%s' % (self['dbtype'], self['host'], self['user'],
                                self['db'])
    if self.get('port'):
      result += ':%d' % self.get('port')
    return result

  def StringWithPasswd(self):
    """Returns the dbspec string for this instance with the password."""
    result = '%s:%s:%s:%s:%s' % (self['dbtype'], self['host'], self['user'],
                                 self['passwd'], self['db'])
    if self.get('port'):
      result += ':%d' % self.get('port')
    return result

  def __iter__(self):
    """Iterate over the dbspecs, expanding hosts, dbs and passwords."""
    for _, host, db in self._expander():
      args = self.copy()
      args['db'] = db
      args['host'] = host
      yield type(self)(**args)

  def Connect(self, connection_class=None):
    connection_class = connection_class or MultiConnection
    self._FetchPassword()
    return connection_class(**self)


def Connect(spec, **kwargs):
  """Connect to a database.

  See Spec for a description of the accepted spec.

  Args:
    spec: The dbspec to connect to (see above)
    **kwargs: Additional arguments are passed to Spec().
      See also the docs for the following:
        Spec.__init__()
        QueryConsumer.__init__()
        MySQLdb.connect()
  Returns:
    A Connection or MultiConnection object.
  """

  return Spec.Parse(spec, **kwargs).Connect()


class Literal(object):
  """Representation of a SQL literal that should not be quoted."""

  def __init__(self, expr):
    """Constructor.

    Args:
      expr: SQL expression to be passed through without quoting.
    """
    self.expr = expr

  def __eq__(self, y):
    return isinstance(y, Literal) and self.expr == y.expr

  def __hash__(self):
    return hash(self.expr)


class OnShard(object):
  """Representation of a SQL statement to be executed on a subset of shards."""

  SHARD_RE = re.compile(
      r'^(?P<leading_comments>(\s*(--\s.*)?\n)*)'
      r'(?P<prefix>\s*ON\s+SHARD\s+(?P<shard>[\d,]+)\s+)'
      r'(?P<query>(.|\n)*)$',
      re.IGNORECASE)

  ALL_SHARDS = frozenset([-1])

  def __init__(self, shards, query):
    self.shards = set(shards)
    self.query = query

  def GetPrefix(self):
    """Return an ON SHARD prefix for this query."""
    if self.shards == self.ALL_SHARDS:
      return ''
    else:
      return 'ON SHARD %s ' % ','.join(str(shard) for shard in self.shards)

  @classmethod
  def FromString(cls, query):
    if isinstance(query, cls):
      # Allow us to be called blindly on valid query objects.
      return query

    shard_match = cls.SHARD_RE.match(query)
    if shard_match:
      shards = (int(shard) for shard in shard_match.group('shard').split(','))
      query = shard_match.group('query')
    else:
      shards = cls.ALL_SHARDS

    return cls(shards, query)


class VirtualTable(object):
  """A class to hold a SQL query result.

  VirtualTable objects store the result internally as a list of field names and
  rows as tuples, but pretend to the world to be full-fledged dictionary
  cursors. This is just a memory-saving hack.

  Using a VirtualTable in an iterative context switches it to streaming mode,
  where it will only be iterable once. If you don't want this behavior, call
  Populate() first.
  """

  _contents = 'Rows'

  def __init__(self, fields, result, rowcount=None, types=None):
    """Constructor.

    Args:
      fields: A list of field names
      result: An iterator of rows with cell data
      rowcount: Number of rows affected by the query. Ignored if result
                is non-empty.
      types: A list of python type classes for fields.
    """
    self._fields = fields
    self._result = iter(result)
    self._types = types or []
    self._rowcount = rowcount
    self._rows = []
    self._started_iteration = False
    self._populated = False

  def Populate(self):
    """Consume all input rows."""
    if self._populated:
      return
    assert not self._started_iteration
    for row in self._result:
      self._Append(row)
    self._populated = True

  def __getitem__(self, i):
    if isinstance(i, slice):
      return VirtualTable(self.GetFields(), self.GetRows()[i],
                          types=self.GetTypes())
    return dict(zip(self._fields, self.GetRows()[i]))

  def __iter__(self):
    if self._populated:
      # Reset
      self._result = iter(self._rows)
    return self

  def next(self):
    self._started_iteration = True
    row = next(self._result)
    if len(row) != len(self._fields):
      raise TypeError('Incorrect column count')
    return dict(zip(self._fields, row))

  def __len__(self):
    return len(self.GetRows())

  def __eq__(self, y):
    return (self.__class__ == y.__class__ and
            self.GetFields() == y.GetFields() and
            self.GetRows() == y.GetRows())

  def __ne__(self, y):
    return not self.__eq__(y)

  def __str__(self):
    rows = []
    for row in self.GetRows():
      fields = ['%s: %s' % x for x in zip(self._fields, row)]
      rows.append('\n'.join(fields))
    return '%s returned: %d\n*****\n%s\n' % (
        self._contents, len(self), '\n*****\n'.join(rows))

  def __sub__(self, y):
    x_rows = set(tuple(row) for row in self.GetRowIter())
    y_rows = set(tuple(row) for row in y.GetRowIter())
    return VirtualTable(self.GetFields(),
                        x_rows - y_rows,
                        types=self.GetTypes())

  def Diff(self, y):
    """Find the differences between this table and another.

    Args:
      y: The other VirtualTable instance.

    Returns:
      A tuple containing two VirtualTable objects. The first contains rows in
      this table and not in y, and the second contains rows in y and not in this
      table.
    """
    x_rows = set(tuple(row) for row in self.GetRowIter())
    y_rows = set(tuple(row) for row in y.GetRowIter())
    return (VirtualTable(self.GetFields(),
                         x_rows - y_rows,
                         types=self.GetTypes()),
            VirtualTable(self.GetFields(),
                         y_rows - x_rows,
                         types=self.GetTypes()))

  def GetTable(self, yield_field_names=True):
    """Generates formatted rows of an output table with fixed-width columns."""
    widths = [max([len('%s' % row[i]) for row in self.GetRows()]
                  + [len(self._fields[i])])
              for i in xrange(len(self._fields))]
    fmts = {
        int: '%%%ds',
        long: '%%%ds',
        float: '%%%ds',
        decimal.Decimal: '%%%ds',
    }
    if self._types:
      types = self._types
    elif self.GetRows():
      types = [type(field) for field in self.GetRows()[0]]
    else:
      types = [str] * len(widths)
    fmt = ' '.join(fmts.get(types[i], '%%-%ds') % width
                   for i, width in enumerate(widths))
    if yield_field_names:
      yield (fmt % tuple(self._fields)).rstrip()
    for row in self.GetRows():
      my_row = list(row)
      for i, value in enumerate(my_row):
        if isinstance(value, str):
          my_row[i] = value.decode('ascii', 'replace')
      yield (fmt % tuple(my_row)).rstrip()

  def __hash__(self):
    """Ordered hash of field names and unordered hash of rows."""
    ret = hash(tuple(self._fields))
    for row in self.GetRows():
      ret ^= hash(tuple(row))
    return ret

  def Append(self, row):
    self.Populate()
    self._Append(row)

  def _Append(self, row):
    """Append a row to the table."""
    if len(row) != len(self._fields):
      raise TypeError('Incorrect column count')
    if isinstance(row, tuple):
      row = list(row)
    self._rows.append(row)

  def AddField(self, name, value):
    """Add a field to the table and fill all cells with value."""
    self.AddFields((name, value))

  def AddFields(self, *args):
    """Add multiple fields to the table and fill all cells with values.

    Each arg for AddFields is a tuple of (name, value) pairs, where name is the
    field name and value is the value to populate the cells with. Fields will be
    added in the order they are passed in as args.

    Raises:
      ValueError: If one of the field names already exists.
    """
    names, values = zip(*args)
    existing_fields = [n for n in names if n in self._fields]
    if existing_fields:
      error = ('Field %s already exists' if len(existing_fields) == 1
               else 'Fields %s already exist')
      raise ValueError(error % ', '.join(existing_fields))
    if isinstance(self._fields, tuple):
      self._fields = list(self._fields)
    self._fields.extend(names)

    old_rows = self.GetRowIter()
    values = tuple(values)

    def RowIter():
      for row in old_rows:
        if not isinstance(row, tuple):
          row = tuple(row)
        yield row + values

    self._result = RowIter()
    self._rows = []
    self._populated = False

  def RemoveField(self, name):
    """Remove a field from the table and delete all of that field's cells."""
    self.RemoveFields(name)

  def RemoveFields(self, *args):
    """Remove multiple fields from the table and delete the associated cells.

    Each arg for RemoveFields is a list of field names to remove. All field
    names must exist or an error is raised.

    Raises:
      ValueError: If one of the field names does not exist.
    """
    invalid_fields = set(args) - set(self._fields)
    if invalid_fields:
      error = ('Field %s does not exist' if len(invalid_fields) == 1
               else 'Fields %s do not exist')
      raise ValueError(error % ', '.join(invalid_fields))
    # Pop result list items in reverse order so the indexes stay correct.
    idxs = [self._fields.index(arg) for arg in reversed(args)]
    self._fields = [field for field in self._fields if field not in args]

    old_rows = self.GetRowIter()

    def RowIter():
      for row in old_rows:
        for i in idxs:
          row.pop(i)
        yield row

    self._result = RowIter()
    self._rows = []
    self._populated = False

  def GetFields(self):
    """Get the list of fields from this result.

    Returns:
      A list of string field names.
    """
    return self._fields

  def GetRows(self):
    """Get the raw, writable row data from this result.

    Returns:
      A list of lists containing cell data.
    """
    self.Populate()
    return self._rows

  def GetRowIter(self):
    """Get an iterator over rows.

    Like iterating over the object itself, calling this may consume a streaming
    input iterator and make future operations on this VirtualTable return no
    rows and log a warning.

    Returns:
      An iterator over the rows of the table.
    """
    if self._populated:
      return iter(self._rows)
    else:
      return self._result

  def GetTypes(self):
    """Get the list of Python types for fields."""
    return self._types

  def GetRowsAffected(self):
    """Get the number of rows affected by the query.

    Returns:
      For SELECT queries, the number of rows returned.
      For other queries, the number of rows inserted, updated or deleted.
      If the query produced an error or warning, returns None.
    """
    return self._rowcount

  def GetDeleteSQLList(self, table_name, fields=None):
    """Turn this table into SQL that would be required to delete all rows.

    Args:
      table_name: The name to delete data from
      fields: A list of fields in a unique key. If not provided, all fields will
        be referenced in the where clauses.

    Yields:
      A list of SQL commands to be executed.
    """
    yielded = False

    fields = fields or self.GetFields()

    for row in self:
      where = ' AND '.join('%s=%s' % (field, _BaseConnection.Escape(row[field]))
                           for field in fields)
      yield 'DELETE FROM %s WHERE %s;' % (table_name, where)

    if not yielded:
      yield '-- No rows to delete from %s' % table_name

  def GetInsertSQLList(self, table_name, max_size=None, extended_insert=True,
                       on_duplicate_key_update=False):
    """Turn this table into SQL that would be required to recreate it.

    Args:
      table_name: The name to insert the data in to
      max_size: The maximum size, in bytes, to make each output query; 0 or None
        for unlimited. Note that a statement can always be longer than max_size
        if max_size is smaller than any single value.
      extended_insert: If false, one insert per line.
      on_duplicate_key_update: Insert will behave as updates when the insert
        would have caused a duplicate value on a UNIQUE or PRIMARY key.

    Yields:
      A list of SQL commands to be executed.
    """
    if not max_size:
      max_size = float('Inf')

    header = 'INSERT INTO %s (%s) VALUES ' % (
        table_name, ','.join(self._fields))
    footer = ';'
    if on_duplicate_key_update:
      updates = ['%s = VALUES(%s)' % (f, f) for f in self._fields]
      footer = ' ON DUPLICATE KEY UPDATE %s;' % ', '.join(updates)

    header_footer_len = len(header) + len(footer)

    rows = []  # Accumulated rows that will be pushed in the next statement.
    statement_len = header_footer_len

    def MakeStatement():
      """Returns a statement made of the header, rows and footer."""
      return header + ','.join(rows) + footer

    yielded = False
    for row in self.GetRowIter():
      next_values = '(%s)' % _BaseConnection.Escape(row)
      # Should we start a new statement?
      if rows:  # Never start an empty statement.
        # The "+ 1" is for the separating comma.
        next_statement_len = statement_len + 1 + len(next_values)
        if not extended_insert or next_statement_len >= max_size:
          yield MakeStatement()
          del rows[:]
          statement_len = header_footer_len
          yielded = True
      rows.append(next_values)
      statement_len += 1 + len(next_values)  # + 1 for the separating comma

    if rows:
      yield MakeStatement()  # There are still unyielded rows.
    elif not yielded:
      yield '-- No rows to insert into %s' % table_name

  def GetInsertSQL(self, table_name):
    """Turn this table into SQL that would be required to recreate it."""
    return '\n'.join(self.GetInsertSQLList(table_name))

  def Merge(self, table):
    """Merge the contents of another VirtualTable."""
    if self.GetFields() != table.GetFields():
      raise TypeError("Field lists don't match (%s vs. %s)" %
                      (self.GetFields(), table.GetFields()))
    self._result = itertools.chain(self.GetRowIter(), table.GetRowIter())
    self._rows = []
    self._populated = False
    # If this table has no rowcount, adopt the other table's value
    other_rowcount = table.GetRowsAffected()
    if self._rowcount is None or other_rowcount is None:
      self._rowcount = other_rowcount
    else:
      self._rowcount += other_rowcount


class QueryErrors(VirtualTable):
  """Hold SQL errors in a table format."""

  _contents = 'Errors'

  def __init__(self, code, msg):
    VirtualTable.__init__(self, ('Code', 'Message'), ((code, msg),))


class QueryWarnings(VirtualTable):
  """Hold SQL warnings in a table format."""

  _contents = 'Warnings'

  def __init__(self, code, msg):
    VirtualTable.__init__(self, ('Code', 'Message'), ((code, msg),))


class Operation(object):
  """An operation that can block between threads."""

  def __init__(self, query):
    self.query = query
    self._event = threading.Event()
    self._result = None
    self._canceled = False

  def SetDone(self, result):
    self._result = result
    self._event.set()

  def Wait(self):
    while not self._event.is_set():
      # This is a workaround against the fact that wait eats
      # KeyboardInterrupt exceptions if it has no timeout.
      self._event.wait(sys.maxint)
    return self._result

  def TryWait(self):
    return self._event.is_set()

  def MarkCanceled(self):
    self._canceled = True

  def IsCanceled(self):
    return self._canceled


class _BaseConnection(object):
  """Common methods for connection objects."""

  _SAFE_TYPES = frozenset((int, long, float, decimal.Decimal))

  def __init__(self, **kwargs):
    self._closed = True
    self._pool = None
    # We strip the last 2 frames (one for BaseConnection and one for the
    # implementation class constructor).
    self._creation = [x.rstrip() for x in traceback.format_stack()[:-2]]
    self._args = kwargs
    if 'passwd' in self._args:
      self._args['passwd'] = '******'
    self._cache = {}
    self._weakref = weakref.ref(self)
    CONNECTIONS.add(self._weakref)

  def __del__(self):
    """Destructor."""
    if not self._closed:
      logging.error('Implicitly closed database handle, created here:\n%s',
                    '\n'.join(self._creation))
    self.Close()
    CONNECTIONS.remove(self._weakref)

  def SetConnectionPool(self, pool):
    """Register a pool to release to."""
    self._pool = pool

  def __enter__(self):
    """'with' keyword begins."""
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    """'with' keyword ends."""
    if self._pool:
      self._pool.Release(self)
      # pool will close if necessary
    else:
      self.Close()

  def __str__(self):
    return '\n'.join([
        str(self.__class__),
        '  Status:',
        '    ' + {True: 'closed', False: 'open'}[self._closed],
        '  Arguments:',
        '    ' + pprint.pformat(self._args).replace('\n', '\n    '),
        '  Created at:',
        '    ' + '\n    '.join(self._creation),
        '=' * 80,
    ])

  @classmethod
  def Escape(cls, value):
    """Escape MySQL characters in a value and wrap in quotes."""
    if value is None:
      return 'NULL'
    if isinstance(value, Literal):
      return value.expr
    if type(value) in cls._SAFE_TYPES:
      return '%s' % value
    if isinstance(value, list) or hasattr(value, '__iter__'):
      return ','.join(cls.Escape(x) for x in value)

    return "'%s'" % ('%s' % value).replace("'", "''").replace('\\', '\\\\')

  def Submit(self, query):
    """Submit a query for execution, return an opaque operation handle."""
    raise NotImplementedError

  def Wait(self, op):
    """Return a dictionary of shard -> ResultIterator."""
    raise NotImplementedError

  def TryWait(self, op):
    """Check if Wait() will succeed immediately."""
    raise NotImplementedError

  def Cancel(self, op):
    """Cancel the currently running query."""
    raise NotImplementedError

  def Execute(self, query, params=None):
    """Execute a query on all targets in parallel, return the common result.

    Args:
      query: The SQL query string
      params: A dictionary of named parameters to be escaped and substituted
        into query.

    Returns:
      A VirtualTable, QueryErrors or QueryWarnings instance.

    Raises:
      InconsistentResponses: When different targets return different responses
    """
    results = self.MultiExecute(query, params)
    by_result = {}
    for name, result in results.iteritems():
      by_result.setdefault(result, []).append(name)
    if len(by_result) == 1:
      return results.popitem()[1]
    else:
      text = ''
      for result, names in by_result.iteritems():
        names.sort()
        text += '%s:\n%s' % (names, result)
      raise InconsistentResponses(text)

  def MultiExecute(self, query, params=None):
    """Execute a query on all targets in parallel, return all results.

    Args:
      query: The SQL query string
      params: A dictionary of named parameters to be escaped and substituted
        into query.

    Returns:
      A dictionary of shard -> result, where result is a VirtualTable,
      QueryErrors or QueryWarnings instance and host is a string representation
      of the individual host.
    """
    results = self.BatchMultiExecute([query], [params])
    ret = {}
    for shard, result in results.iteritems():
      ret[shard] = next(result)
    return ret

  def BatchMultiExecute(self, queries, params=None):
    """Execute queries on all targets, return iteratble results.

    Args:
      queries: An iterable of SQL query strings
      params: None, or an iterable of dictionaries to be escaped and substituted
        into queries, at least the length of queries.

    Returns:
      A dictionary of shard -> ResultIterator instance, or list.
    """
    params = params or (None,) * len(queries)
    full_queries = []
    shards = None
    for query, query_params in zip(queries, params):
      if query_params is not None:
        query %= dict(zip(
            query_params.iterkeys(),
            map(self.Escape, query_params.itervalues())))

      query_obj = OnShard.FromString(query)
      if shards:
        if query_obj.shards != shards:
          raise InvalidShardMix('%s vs. %s\n%s vs. %s' %
                                (shards, query_obj.shards, full_queries, query))
      else:
        shards = query_obj.shards

      full_queries.append(query_obj.query.rstrip(';'))

    full_query = ';\n'.join(full_queries)
    op = self.Submit(OnShard(shards, full_query))
    return self.Wait(op)

  def ExecuteMerged(self, query, params=None):
    """Execute a query on all targets in parallel, return all results merged.

    Args:
      query: The SQL query string
      params: A dictionary of named parameters to be escaped and substituted
        into query.

    Returns:
      A merged VirtualTable with consolidated results from all hosts, plus a
      'shard' column indicating where results originated from.

    Raises:
      InconsistentSchema: When different targets return different schema.
      QueryErrorsException: When any target returns errors.
      QueryWarningsException: When any target returns warnings.
    """
    return next(self.BatchExecuteMerged([query], [params]))

  def BatchExecuteMerged(self, queries, params=None):
    """Execute queries and return all results merged.

    Args:
      queries: An iterable of SQL query strings
      params: None, or an iterable of dictionaries to be escaped and substituted
        into queries, at least the length of queries.

    Yields:
      Merged VirtualTables with consolidated results from all hosts, plus a
      'shard' column indicating where results originated from.

    Raises:
      InconsistentSchema: When different targets return different schema.
      QueryErrorsException: When any target returns errors.
      QueryWarningsException: When any target returns warnings.
    """
    results = self.BatchMultiExecute(queries, params)

    while True:
      merged = None
      for shard, shard_results in results.iteritems():
        # We'll leave this function when this yields StopIteration and it raises
        result = next(shard_results)

        if isinstance(result, QueryErrors):
          raise QueryErrorsException(result)
        if isinstance(result, QueryWarnings):
          raise QueryWarningsException(result)
        if result is None and merged is None:
          # Might be the result of a query that returns no data
          continue
        result.AddField('shard', shard)
        if merged:
          # Verify that the field list from this host is the same as all that
          # came before.
          if result.GetFields() != merged.GetFields():
            raise InconsistentSchema(
                '%s vs. %s' % (result.GetFields(), merged.GetFields()))
        else:
          # First time through the loop, create a new result table.
          merged = VirtualTable(result.GetFields(), [], types=result.GetTypes())
        merged.Merge(result)
      yield merged

  def ClearCache(self):
    self._cache.clear()

  def CachedExecute(self, query, params=None):
    """Execute() with a caching layer to execute each query only once."""
    if params is not None:
      # We have to merge params before we check the cache.
      query %= dict(zip(params.keys(), map(self.Escape, params.values())))
    if query not in self._cache:
      result = self.Execute(query)
      result.Populate()
      self._cache[query] = result
    return self._cache[query]

  def ExecuteOrDie(self, query, params=None, execute=None):
    """Execute() a query and raise an exception on failure."""
    result = (execute or self.Execute)(query, params)
    if isinstance(result, QueryErrors):
      raise QueryErrorsException(result)
    if isinstance(result, QueryWarnings):
      raise QueryWarningsException(result)
    return result

  def CachedExecuteOrDie(self, query, params=None):
    """Combination of CachedExecute() and ExecuteOrDie()."""
    return self.ExecuteOrDie(query, params, execute=self.CachedExecute)

  def ExecuteWithRetry(self, query, params=None, execute=None, max_attempts=5,
                       start_delay=1, backoff_multiplier=2):
    """Execute a query and retry on fatal errors."""
    execute = execute or self.ExecuteOrDie
    for attempt in xrange(max_attempts):
      try:
        return execute(query, params)
      except QueryErrorsException:
        logging.exception('Retryable error')
      time.sleep(start_delay * (backoff_multiplier ** attempt))
    raise RetriesExceeded

  def Close(self):
    """Close database connections to all targets.

    This MUST be called before the handle is implicitly destroyed, or we log an
    error (to encourage closing ASAP after use completion).
    """
    self.Submit('exit')
    self.ClearCache()
    self._closed = True

  def Transaction(self, *args, **kwargs):
    """Factory for a Transaction object."""
    return Transaction(self, *args, **kwargs)

  def Lock(self, *args, **kwargs):
    """Factory for a Lock object."""
    return Lock(self, *args, **kwargs)


class RowIterator(object):
  """Pythonic iterator over the rows of a single MySQL result set."""

  def __init__(self, result):
    self._result = result
    self._queue = Queue.Queue(maxsize=100)

  def Delete(self):
    """Consume (and discard) all rows to free up the MySQL connection."""
    if not self._queue:
      return
    for _ in self:
      pass

  def __del__(self):
    self.Delete()

  def __iter__(self):
    return self

  def PushRows(self):
    """Run in the MySQL thread to push rows onto the queue."""
    while True:
      row = self._result.fetch_row()
      if not row:
        self._queue.put(None)
        break
      self._queue.put(row)

  def next(self):
    """Return the next row or raise StopIteration."""
    if not self._queue:
      raise MultipleIteration('Iterating over a consumed RowIterator. '
                              'First iteration at:\n%s' %
                              '\n'.join(self._iter_stack))
    row = self._queue.get()
    if row is None:
      self._iter_stack = [x.rstrip() for x in traceback.format_stack()[:-2]]
      self._queue = None
      raise StopIteration
    return row[0]


class ResultIterator(object):
  """Pythonic iterator over MySQL result sets."""

  _TYPES = {
      0: float,
      1: int,
      2: int,
      3: int,
      4: float,
      5: float,
      8: int,
      9: int,
      246: float,
      249: str,
      250: str,
      251: str,
      252: str,
      253: str,
      254: str,
  }

  def __init__(self, dbh, charset, stream_results, fatal_errors):
    """Constructor.

    Args:
      dbh: MySQLdb database connection handle.
      charset: Name of character set to interpret results in.
      stream_results: Boolean; if True, fetch and process row-at-a-time,
        otherwise buffer full result.
      fatal_errors: Set of integer error codes to be treated as fatal to the
        connection.
    """
    self._dbh = dbh
    self._charset = charset
    self._stream_results = stream_results
    self._fatal_errors = fatal_errors
    self._queue = Queue.Queue(maxsize=100)
    self._last_result = None

  def Delete(self):
    """Consume (and discard) all results to free up MySQL connection."""
    if not self._queue:
      return
    for _ in self:
      pass

  def __del__(self):
    self.Delete()

  def __iter__(self):
    return self

  def PushResults(self):
    """Run in the MySQL thread to push results onto the queue.

    Returns:
      True on pushing all results to the queue. False on failure that requires
      the connection to be closed.
    """
    ret = True
    while True:
      try:
        if self._stream_results:
          result = self._dbh.use_result()
        else:
          result = self._dbh.store_result()
        rowcount = self._dbh.affected_rows()
        # Check errno and raise an exception, because MySQLdb isn't doing it for
        # us. b/7726216
        if self._dbh.errno():
          raise MySQLdb.Error(self._dbh.errno(), self._dbh.error())
        if self._dbh.warning_count() > 0:
          msg = 'Query produced warnings; run SHOW WARNINGS for details'
          self._queue.put((None, QueryWarnings(5, msg)))
          continue
        if not result:
          # Query returned no rows, but might have affected some
          self._queue.put((None, VirtualTable([], [], rowcount, types=[])))
          continue
        # Query returned some rows
        fields = [i[0].decode(self._charset) for i in result.describe()]
        types = [self._TYPES.get(i[1], None) for i in result.describe()]
        rowiter = RowIterator(result)
        vt = VirtualTable(fields, rowiter, rowcount, types=types)
        self._queue.put((rowiter, vt))
        rowiter.PushRows()
      except MySQLdb.Error as e:
        code, message = e.args
        logging.exception('Query returned error.')
        self._queue.put((None, QueryErrors(code, message)))
        if code in self._fatal_errors:
          ret = False
      except Exception as e:
        logging.exception('Query returned unknown error.')
        self._queue.put((None, QueryErrors(4, str(e))))
      finally:
        # next_result() has a C-style API; 0 on success, -1 on failure.
        try:
          if self._dbh.next_result():
            self._queue.put((None, None))
            break
        except MySQLdb.Error as e:
          # next_result threw an error. This means it'll never work, so stop
          # trying for this query.
          code, message = e.args
          logging.exception('Query returned error.')
          self._queue.put((None, QueryErrors(code, message)))
          if code in self._fatal_errors:
            ret = False
          self._queue.put((None, None))
          break

    return ret

  def FeedFinalResult(self, result):
    """Manually feed a final result into the iterator."""
    self._queue.put((None, result))
    self._queue.put((None, None))

  def next(self):
    """Return the next row or raise StopIteration."""
    if not self._queue:
      raise MultipleIteration('Iterating over a consumed ResultIterator. '
                              'First iteration at:\n%s' %
                              '\n'.join(self._iter_stack))
    if self._last_result is not None:
      self._last_result.Delete()
    self._last_result, value = self._queue.get()
    if value is None:
      self._queue = None
      self._iter_stack = [x.rstrip() for x in traceback.format_stack()[:-2]]
      raise StopIteration
    return value


class QueryConsumer(threading.Thread):
  """Consume SQL queries from a queue and return results."""

  _ERR_QUERY_CANCELED = QueryErrors(2, 'Query canceled')
  _ERR_UNKNOWN = QueryErrors(3, 'Unknown problem')

  _DBTYPE_SETUP = {
      'mysql': None,
  }

  def __init__(self, execute_on_connect=(), stream_results=False,
               fatal_errors=(1142, 1143, 1148, 2003, 2006, 2013, 2014),
               **kwargs):
    """Create a new QueryConsumer.

    Args:
      execute_on_connect: List of SQL statements to execute after connecting.
      stream_results: Whether to stream results from MySQL; default is False.
        See ResultIterator.
      fatal_errors: Set of integer error codes to be treated as fatal.
        See ResultIterator.
      **kwargs: Additional arguments (typically from Spec):
        'dbtype' - Required to setup the connection; must be in _DBTYPE_SETUP.
        Other arguments are passed to MySQLdb.connect() when connecting.
    """
    threading.Thread.__init__(self)
    self.setDaemon(True)
    self._execute_on_connect = execute_on_connect
    self._stream_results = stream_results
    self._fatal_errors = fatal_errors
    self._charset = kwargs.get('charset', 'utf8')
    self._dbargs = kwargs
    self._dbh = None
    self._queue = Queue.Queue(0)
    self.connection_info = None
    self.in_progress = None
    self._resolver = None

    if 'host' in self._dbargs:
      self.setName(self._dbargs['host'])
      self._resolver = GetResolver(self._dbargs)

  def _HandleDeathPills(self, src):
    """Handle special local commands from the query stream."""
    for op in src:
      is_exit = op.query in ('exit', 'exit;')
      is_destroy = op.query in ('destroy', 'destroy;')
      if is_exit or is_destroy:
        self._Close()
        op.SetDone([None])
        if is_destroy:
          break
        else:
          continue

      yield op

  def _SkipCanceled(self, src):
    """Filter out canceled queries from the stream."""
    for op in src:
      self.in_progress = op
      if op.IsCanceled():
        logging.debug('Not executing canceled query %s', op.query)
        op.SetDone([self._ERR_QUERY_CANCELED])
        self.in_progress = None
        continue

      yield op

      self.in_progress = None

  def _ConnectIfNecessary(self):
    """Connect if we're not already connected.

    Returns:
      A QueryErrors object, or None on success.
    """
    if not self._dbh:
      args = self._dbargs.copy()
      if self._resolver:
        try:
          (args['host'], port) = random.choice(self._resolver())
        except ResolutionError, e:
          logging.exception('Resolution failure.')
          return QueryErrors(1, str(e))
        if not args.get('port'):
          args['port'] = port
      try:
        self._Connect(args)
      except MySQLdb.OperationalError, e:
        logging.exception('Connection returned error.  DB:%s:%s',
                          args['host'], args.get('port'))
        self._Close()
        return QueryErrors(e[0], e[1])
      except Exception, e:
        logging.exception('Connection returned unknown error. DB:%s:%s',
                          args['host'], args.get('port'))
        self._Close()
        return QueryErrors(3, str(e))

  def _Execute(self, src):
    """Actually execute the query on the database."""
    last_response = None

    for op in src:
      connection_error = self._ConnectIfNecessary()

      if last_response:
        last_response.Delete()
      last_response = ResultIterator(self._dbh,
                                     self._charset,
                                     self._stream_results,
                                     self._fatal_errors)

      if connection_error:
        op.SetDone(last_response)
        last_response.FeedFinalResult(connection_error)
        continue
      try:
        query = op.query
        if isinstance(query, unicode):
          query = query.encode(self._charset)
        logging.debug('Executing %s', query)
        self._dbh.query(query)
      except MySQLdb.Error, e:
        code, message = e.args
        logging.exception('Query returned error.')
        if code in self._fatal_errors:
          self._Close()
        op.SetDone(last_response)
        last_response.FeedFinalResult(QueryErrors(code, message))
        continue
      except Exception, e:
        logging.exception('Query returned unknown error.')
        op.SetDone(last_response)
        last_response.FeedFinalResult(QueryErrors(4, str(e)))
        continue
      op.SetDone(last_response)
      if not last_response.PushResults():
        self._Close()

  def run(self):
    """Main loop inside the consumer thread."""
    # Set up a pipeline to handle operations from the queue. Each of the steps
    # is a generator except the last, so the last call to func() below blocks
    # until the queue drains.
    self._Execute(
        self._SkipCanceled(
            self._HandleDeathPills(
                iter(self._queue.get, None))))

  def _Connect(self, args):
    log_args = args.copy()
    if 'passwd' in log_args:
      log_args['passwd'] = 'XXXXXXX'
    logging.debug('Connecting with %s', log_args)

    setup = self._DBTYPE_SETUP[args['dbtype']]
    if setup:
      setup(args)
    del args['dbtype']  # MySQLdb doesn't like extra arguments.

    self._dbh = MySQLdb.connect(**args)
    self._dbh.autocommit(True)
    self._dbh.query('SELECT CONNECTION_ID()')
    data = self._dbh.store_result()
    self.connection_info = {'args': args,
                            'id': int(data.fetch_row(0)[0][0])}
    for init_query in self._execute_on_connect:
      logging.debug('Executing on-connect query: %s', init_query)
      self._dbh.query(init_query)
      self._dbh.store_result()

  def _Close(self):
    if self._dbh:
      logging.debug('Closing connection to %s', self._dbargs['host'])
      self._dbh.close()
      self._dbh = None
      self.connection_info = None

  def Submit(self, op):
    self._queue.put(op)


class Connection(_BaseConnection):
  """A connection to a single database host."""

  def __init__(self, **kwargs):
    """Create a new connection.

    At least "dbtype" is required in keyword arguments. Some others may be
    required, depending on the specifics of the connection.

    Args:
      **kwargs: Additional arguments are passed to QueryConsumer.
    """
    _BaseConnection.__init__(self, **kwargs)
    self._consumer = QueryConsumer(**kwargs)
    self._consumer.start()

  def __del__(self):
    self.Wait(self.Submit('destroy'))
    self._consumer.join()
    _BaseConnection.__del__(self)

  def Submit(self, query):
    if isinstance(query, OnShard):
      assert len(query.shards) == 1, 'Multi-shard query passed to single shard'
      query = query.query
    op = Operation(query)
    self._consumer.Submit(op)
    return op

  def Wait(self, op):
    return {0: op.Wait()}

  def TryWait(self, op):
    return op.TryWait()

  def Cancel(self, op):
    """Cancel a pending or running operation, if possible.

    Args:
      op: The opaque handle returned by the Submit() call.
    """
    op.MarkCanceled()
    if self._consumer.in_progress != op: return
    while not op.TryWait():  # wait until our query completes
      connection_info = self._consumer.connection_info
      if not connection_info:
        time.sleep(0.1)
        continue
      try:
        temp_dbh = MySQLdb.connect(**connection_info['args'])
        temp_dbh.query('KILL QUERY %d' % connection_info['id'])
        temp_dbh.close()
      except MySQLdb.Error, e:
        logging.error('Failed to cancel query: %s', e)
      time.sleep(0.1)


class MultiConnection(_BaseConnection):
  """Wrap a set of real connections; execute in parallel."""

  def __init__(self, **kwargs):
    _BaseConnection.__init__(self, **kwargs)
    spec_template = Spec(**kwargs)
    self._connections = {}
    for i, spec in enumerate(spec_template):
      # Make a copy before we add to it, in case the list is shared across
      # specs.
      spec['execute_on_connect'] = list(spec.get('execute_on_connect', []) +
                                        ['SET @shard=%d' % i])
      self._connections[i] = [spec, None]

  def __del__(self):
    for _, connection in self._connections.itervalues():
      connection.Close()
    _BaseConnection.__del__(self)

  def Submit(self, query):
    """Submit a query for execution without blocking for completion.

    Args:
      query: SQL query string.
    Returns:
      An opaque handle to the running query, to be passed to Wait() or Cancel().
    Raises:
      InvalidShard: When a statement references a shard number which is not part
        of this connection.
    """
    query_obj = OnShard.FromString(query)

    if query_obj.shards == OnShard.ALL_SHARDS:
      query_obj.shards = set(self._connections)
    else:
      if not query_obj.shards.issubset(set(self._connections)):
        raise InvalidShard('%s is not a subset of %s'
                           % (query_obj.shards, self._connections.keys()))

    ops = []
    for shard, (spec, connection) in self._connections.iteritems():
      if shard in query_obj.shards:
        if connection is None:
          connection = spec.Connect(connection_class=Connection)
          self._connections[shard] = spec, connection
        ops.append((shard, connection, connection.Submit(query_obj.query)))
    return ops

  def Wait(self, ops):
    results = {}
    for name, connection, op in ops:
      results[name] = connection.Wait(op).values()[0]
    return results

  def TryWait(self, ops):
    for _, _, op in ops:
      if not op.TryWait():
        return False
    return True

  def Cancel(self, op):
    for _, connection, subop in op:
      connection.Cancel(subop)


class ConnectionPool(_BaseConnection):
  """Thread-safe self-resizing pool of connections."""

  def __init__(self, spec, max_open_unused=1, max_open=5, **kwargs):
    """Constructor.

    Args:
      spec: dbspec; see Connect()
      max_open_unused: Maximum number of connections to keep open and unused in
        the pool.
      max_open: Maximum number of connections total.  If the dbspec is to
        multiple shards (a MultiConnection underneath), this is the number of
        connections *per shard*.
      **kwargs: Additional arguments are passed to db.Connect().
    """
    _BaseConnection.__init__(self)
    self._max_open_unused = max_open_unused
    self._cv = threading.Condition()
    self._open_spares = []    # GUARDED_BY(_cv)
    self._closed_spares = []  # GUARDED_BY(_cv)
    # We create all connections now, but don't connect them.  Connection objects
    # don't open connections until their first use.
    for _ in xrange(max_open):
      self._closed_spares.append(Connect(spec, **kwargs))

  def IsAvailable(self):
    """Check if sending a query won't block for connection limit.

    This value is out-of-date before this function returns, so only use it in
    advisory capacities.

    Returns:
      True if a connection is available at the moment of the check, otherwise
      False.
    """
    return (len(self._open_spares) + len(self._closed_spares)) > 0

  def Close(self):
    # Close every connection that has been returned.  Those still checked out
    # are lost.
    with self._cv:
      for conn in self._open_spares + self._closed_spares:
        conn.Close()
      self._closed = True

  def Acquire(self):
    """Get a connection from the pool.

    Blocks if there are no spare connections.  You must call Release() when you
    are done with the connection, or it will orphaned and not be usable by other
    pool callers.

    Returns:
      A Connection or MultiConnection instance.
    """
    with self._cv:
      while not self._open_spares and not self._closed_spares:
        logging.info('ConnectionPool blocking waiting for a connection.')
        start_time = time.time()
        self._cv.wait()
        logging.info('ConnectionPool waited %f seconds to get a connection.',
                     time.time() - start_time)
      try:
        conn = self._open_spares.pop()
      except IndexError:
        conn = self._closed_spares.pop()
      conn.SetConnectionPool(self)
      return conn

  def Release(self, conn):
    """Return a connection to the pool.

    The caller may not use the conn object after calling Release().

    Args:
      conn: The connection instance to return.
    """
    conn.SetConnectionPool(None)
    with self._cv:
      if len(self._open_spares) < self._max_open_unused:
        self._open_spares.append(conn)
      else:
        conn.Close()
        self._closed_spares.append(conn)
      self._cv.notify()

  def Submit(self, query):
    """Submit a query for execution, return an opaque operation handle."""
    conn = self.Acquire()
    return [conn, conn.Submit(query)]

  def Wait(self, op):
    """Return a dictionary as described in MultiExecute()."""
    conn, opobj = op
    ret = conn.Wait(opobj)
    self.Release(conn)
    return ret

  def TryWait(self, op):
    """Return true if the operation has completed."""
    conn, opobj = op
    return conn.TryWait(opobj)

  def Cancel(self, op):
    conn, subop = op
    conn.Cancel(subop)


# Matches, e.g., {0..89}
_RANGE_RE = re.compile(r'{(?P<start>\d+)\.\.(?P<end>\d+)}')


def _GetExpander(name, dbargs):
  if '#' in name:
    expander_class = _HashExpander
  elif ',' in name:
    expander_class = _ListExpander
  elif _RANGE_RE.search(name):
    expander_class = _RangeExpander
  else:
    expander_class = _NoOpExpander
  return expander_class(name, dbargs)


def GetResolver(dbspec):
  """Create a resolver suitable to the given name.

  Args:
    dbspec: a db.Spec instance to resolve into one or more (ip, port) pairs.
      Port selection is determined by dbspec data. For example: the hostname
      (dbspec['host']) may contain a port number (e.g. 'name:port').

  Returns:
    A Cache object that returns the resolver result: [(ip, port)]
  """

  # If it's nothing else, we assume that it's DNS
  return DNSResolver(dbspec['host'])


class Cache(object):
  """Simple wrapper to store args and cache result."""

  # Entirely arbitrary value
  _CACHE_TTL = 60

  def __init__(self, name, args=None):
    self._name = name
    self._args = args
    self._last_lookup_time = 0

  def __call__(self):
    if time.time() - self._last_lookup_time > self._CACHE_TTL:
      self._last_lookup_value = self._Lookup()
    return self._last_lookup_value


def _ExpandDb(db_str, index):
  if not db_str:
    return db_str
  if ',' in db_str:
    return db_str.split(',')[index]
  elif '#' in db_str:
    return db_str.replace('#', str(index))
  else:
    return db_str


class _HashExpander(Cache):
  """Expand # in a name."""

  def _Lookup(self):
    # As long as we remove at least one # from the name, this can't be
    # infinitely recursive.
    shard_0_dbargs = self._args.copy()
    shard_0_dbargs.update({
        'host': self._args['host'].replace('#', '0'),
        'db': self._args['db'].replace('#', '0')
    })

    conn = MultiConnection(**shard_0_dbargs)
    result = conn.ExecuteOrDie('SELECT NumShards FROM ConfigurationGlobals')
    expansion = []
    for x in xrange(int(result[0]['NumShards'])):
      shard_host = self._name.replace('#', str(x))
      db = _ExpandDb(self._args['db'], x)
      expansion.append((x, shard_host, db))
    conn.Close()
    return expansion


class _ListExpander(Cache):
  """Expand , in a name (list of hosts)."""

  def _Lookup(self):
    hosts = self._name.split(',')
    return [(i, host, _ExpandDb(self._args['db'], i))
            for i, host in enumerate(hosts)]


class _RangeExpander(Cache):
  """Expand {0..9} in a name."""

  def _Lookup(self):
    range_result = _RANGE_RE.search(self._name)
    expansion = []
    range_params = range_result.groupdict()
    for x in xrange(int(range_params['start']), int(range_params['end']) + 1):
      host = self._name.replace(range_result.group(0), str(x))
      db = _ExpandDb(self._args['db'], x)
      expansion.append((x, host, db))
    return expansion


class _NoOpExpander(Cache):
  """Expand a name to itself, as shard zero."""

  def _Lookup(self):
    return [(0, self._name, self._args['db'])]


_DEFAULT_PORT = 3306


class DNSResolver(Cache):
  """Resolve a single DNS host."""

  def _Lookup(self):
    if self._name == 'localhost':
      # Hack to allow connecting via the UNIX socket.
      return [('localhost', _DEFAULT_PORT)]
    try:
      ip_list = socket.gethostbyname_ex(self._name)[2]
    except socket.gaierror:
      raise ResolutionError('Failed to resolve %s' % self._name)
    return [(ip, _DEFAULT_PORT) for ip in ip_list]


def XSplit(value, sep, callback=None):
  """Split the input as a generator.

  If specified, fires callback with one argument (character position of end of
  line) after each line is yielded.

  Args:
    value: The string value to split.
    sep: The separator character, typically newline.
    callback: Optional callback function is called with the offset in value of
      the start of each yield value, except the last one in the case that value
      does not end with sep.
  Yields:
    Each word line split from value.
  """
  loc = 0
  while True:
    splitpoint = value.find(sep, loc)
    if splitpoint == -1:
      yield value[loc:]
      return
    yield value[loc:splitpoint]
    loc = splitpoint + len(sep)
    if callback:
      callback(loc)


def XCombineSQL(lines):
  """Combine lines into SQL statements."""
  buf = []
  for line in lines:
    buf.append(line)
    stripped = line.strip()
    if stripped.endswith(';') and not stripped.startswith('-- '):
      statement = '\n'.join(buf).strip()
      buf = []
      yield statement
  # Allow no remaining input, or trailing newlines.
  for line in buf:
    if line.strip() and not _COMMENT_RE.match(line):
      raise InputRemaining(buf)


class Lock(object):
  """Pythonic wrapper for a named database lock.

  WARNING: This lock has very odd behavior. You can acquire multiple times from
  the same database connection, but still only release once. This means that:

  with db.Lock(dbh, 'foo'):
    with db.Lock(dbh, 'foo'):
      pass
    # Code here will be running without the lock
  # LostLock will be thrown when the outer "with" exits
  """

  def __init__(self, dbh, name, seconds_to_wait=999999):
    self._dbh = dbh
    self._name = name
    self._seconds_to_wait = seconds_to_wait

  def __enter__(self):
    result = self._dbh.ExecuteOrDie(
        'SELECT GET_LOCK(%(name)s, %(seconds_to_wait)s) AS l',
        {
            'name': self._name,
            'seconds_to_wait': self._seconds_to_wait,
        })
    if result[0]['l'] != 1:
      if result[0]['l'] == 0:
        raise Timeout('Failed to get named lock "%s"' % self._name)
      else:
        raise Error('Error acquiring lock "%s"' % self._name)
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    result = self._dbh.ExecuteOrDie(
        'SELECT RELEASE_LOCK(%(name)s) AS l', {
            'name': self._name,
            })
    if result[0]['l'] != 1:
      raise LockLost('Lock "%s" lost while holding' % self._name)


class Transaction(object):
  """Pythonic wrapper for a database transaction.

  Example use:
    with db.Transaction(dbh):
      # operate on database

  WARNING: Do not nest transaction objects. Creation of the inner object will
  implicitly commit the outer one, i.e.:

  with db.Transaction(dbh):
    # outer transaction only
    with db.Transaction(dbh):
      # statements from outer transaction implicitly committed
      # inside inner transaction only
    # outside of any transaction
  """

  def __init__(self, dbh):
    self._dbh = dbh

  def __enter__(self):
    self._dbh.ExecuteOrDie('BEGIN')
    return self

  def __exit__(self, unused_type, value, unused_traceback):
    if value:
      self._dbh.ExecuteOrDie('ROLLBACK')
    else:
      self._dbh.ExecuteOrDie('COMMIT')
