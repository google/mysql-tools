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

"""Push and publish MySQL permissions.

Usage::
  from permissions import use
  permissions = 'a = Account()'
  perms = use.PermissionsFile(permissions)
  perms.Push(dbh, 'secondary')
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'
__docformat__ = 'epytext en'

import logging
import threading

try:
  from ..pylib import db
except (ImportError, ValueError):
  from pylib import db

import define
import utils


class Error(Exception):
  """The base class for exceptions in this module."""


class NeedDBAccess(Error):
  """Raised when an action requires a connected database."""


class ValidationFailure(Error):
  """Raised when the expected permissions were not applied."""


# We copy two tables that we don't publish or push to, so copy needs its own
# map.
COPY_MAP = {
    'mysql.columns_priv': 'admin.MysqlPermissionsColumnsPriv',
    'mysql.db':           'admin.MysqlPermissionsDb',
    'mysql.host':         'admin.MysqlPermissionsHost',
    'mysql.mapped_user':  'admin.MysqlPermissionsMappedUser',
    'mysql.tables_priv':  'admin.MysqlPermissionsTablesPriv',
    'mysql.user':         'admin.MysqlPermissionsUser',
    }


def Copy(dbh, set_name, flush=True):
  """Copies the permission set from the admin.* tables to the mysql.* tables.

  Tables copied are::
    admin.MysqlPermissionsColumnsPriv => mysql.columns_priv
    admin.MysqlPermissionsDb          => mysql.db
    admin.MysqlPermissionsHost        => mysql.host
    admin.MysqlPermissionsMappedUser  => mysql.mapped_user
    admin.MysqlPermissionsTablesPriv  => mysql.tables_priv
    admin.MysqlPermissionsUser        => mysql.user

  Note that only rows from admin.* matching set_name are copied.  The contents
  of mysql.* are replaced completely.

  Args:
    dbh: A connected database handle (pylib.db).
    set_name: The permissions set name, e.g. "primary", "secondary".
    flush: If True, activate newly-copied permissions.
  """
  with db.Lock(dbh, 'permissions copy'):
    with db.Transaction(dbh) as trx:
      result = dbh.ExecuteOrDie(
          'SELECT MysqlPermissionSetId,'
          ' UNIX_TIMESTAMP(LastUpdated) AS LastUpdated'
          ' FROM admin.MysqlPermissionSets'
          ' WHERE PermissionSetLabel=%(set_label)s;', {'set_label': set_name})
      set_id = result[0]['MysqlPermissionSetId']
      last_updated = result[0]['LastUpdated']

      result = dbh.ExecuteOrDie(
          'SELECT COUNT(*) AS c FROM admin.MysqlPermissionsUser'
          ' WHERE MysqlPermissionSetId=%(set_id)s', {'set_id': set_id})
      assert result[0]['c'], 'Permissions copy with blank user table'

      for dest, source in sorted(COPY_MAP.iteritems()):
        result = dbh.ExecuteOrDie('DESC %s' % dest)
        columns = [row['Field'] for row in result]
        columns_str = ','.join(columns)
        dbh.ExecuteOrDie('DELETE FROM %s' % dest)
        dbh.ExecuteOrDie('INSERT INTO %s SELECT %s FROM %s'
                         ' WHERE MysqlPermissionSetId=%s' % (
                             dest, columns_str, source, set_id))

      dbh.ExecuteOrDie('DELETE FROM adminlocal.LocalMysqlPermissionState')
      dbh.ExecuteOrDie('INSERT INTO adminlocal.LocalMysqlPermissionState SET'
                       ' MysqlPermissionSetId=%(set_id)s,'
                       ' LastPushed=FROM_UNIXTIME(%(last_pushed)s)',
                       {'set_id': set_id,
                        'last_pushed': last_updated})
    if flush:
      dbh.ExecuteOrDie('FLUSH LOCAL PRIVILEGES')


def GetPermissionSetId(dbh, set_name):
  """Find the ID for a named permission set."""
  result = dbh.ExecuteOrDie(
      'SELECT MysqlPermissionSetId FROM admin.MysqlPermissionSets WHERE'
      ' PermissionSetLabel=%(set_name)s;', {'set_name': set_name})
  return result[0]['MysqlPermissionSetId']


def MarkPermissionsChanged(dbh, set_id, push_duration=1800):
  """Mark a permissions set changed, so clients will recopy it."""
  dbh.ExecuteOrDie(
      'UPDATE admin.MysqlPermissionSets SET'
      ' LastUpdated=NOW(), PushDuration=%(push_duration)s'
      ' WHERE MysqlPermissionSetId=%(set_id)s;', {
          'push_duration': push_duration,
          'set_id': set_id,
          })


class PermissionsFile(object):
  """Represent permissions definitions."""

  _lock = threading.Lock()

  def __init__(self, permissions_contents=None, private_keyfile=None):
    """Load the permissions definitions.

    Args:
      permissions_contents: string containing Python code that defines
        permissions, using the framework provided in define.py.
      private_keyfile: Path to a file containing the private RSA key to be
        used to decrypt encrypted_hash values from permissions_contents.
    """
    self._sets = {}

    if permissions_contents:
      self.Parse(permissions_contents)

    if private_keyfile:
      self._decryption_key = utils.PrivateKeyFromFile(private_keyfile)
    else:
      self._decryption_key = None

  def Parse(self, permissions_contents, define_overrides=None):
    """Parses permissions definitions.

    Args:
      permissions_contents: string containing Python code that defines
        permissions, using the framework provided in define.py.
      define_overrides: An optional dict whose values override the normal
        execution environment from define.py.
    """
    code = compile(permissions_contents, 'permissions contents', 'exec')

    # TODO(flamingcow): There is a race when using two instances of
    # PermissionsFile at once. We need a better way to return values from
    # define.py than a global.
    with self._lock:
      # This is self.globals as a backwards-compat hack, because there are some
      # users of this class that want to get at things from the permissions file
      # other than SETS.
      # TODO(unbrice): Remove the hack, it seems there is only one user anyway.
      self.globals = define.__dict__.copy()
      if define_overrides:
        self.globals.update(define_overrides)
      # Reset define's global state. This makes multiple instantiations of
      # PermissionsFile work.
      self.globals['SETS'].clear()
      self.globals['SETS'].update(self._sets)
      exec code in self.globals
      self._sets = self.globals['SETS'].copy()

  def GetSetTables(self, dbh, set_name):
    """Generate a table set for each account, then merge them all together."""
    if self._decryption_key:
      self._sets[set_name].Decrypt(self._decryption_key)
    return self._sets[set_name].GetTables(dbh)

  @staticmethod
  def _GetTableDiffSQL(dbh, old_table, new_table, name):
    """Generate SQL to apply the difference between two tables."""
    fields = list(old_table.GetFields())
    try:
      db_index = fields.index('Db')
    except ValueError:
      pass
    else:
      for row in old_table.GetRows():
        if row[db_index] == 'DATABASE()':
          row[db_index] = define.DEFAULT
    new_rows, old_rows = new_table.Diff(old_table)
    args = {}
    args['db'], args['table'] = name.split('.', 1)
    query = """
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE
  TABLE_SCHEMA=%(db)s AND
  TABLE_NAME=%(table)s AND
  CONSTRAINT_NAME='PRIMARY'
  ORDER BY ORDINAL_POSITION ASC;
"""
    key_fields = [row['COLUMN_NAME'] for row
                  in dbh.CachedExecuteOrDie(query, args)]
    for sql in old_rows.GetDeleteSQLList(name, fields=key_fields):
      yield sql
    for sql in new_rows.GetInsertSQLList(name, max_size=2**20):
      yield sql

  @classmethod
  def _GetIncrementalTableSQL(cls, dbh, table, name, delete_where):
    """Generate SQL to make incremental changes that result in table."""
    fields = list(table.GetFields())
    try:
      db_index = fields.index('Db')
    except ValueError:
      db_index = -1
    if db_index >= 0:
      fields[db_index] = "IF(Db = DATABASE(), 'DATABASE()', Db) AS Db"

    if not isinstance(dbh, db.MultiConnection):
      # Single shard
      query = 'SELECT %s FROM %s WHERE %s;' % (
          ','.join(fields), name, delete_where)
      old_table = dbh.ExecuteOrDie(query)
      return cls._GetTableDiffSQL(dbh, old_table, table, name)

    # Multiple shards

    # If we have Google MySQL extensions available, verify that all shards
    # match, then only fetch data from one shard.
    fields_copy = list(fields)
    if db_index >= 0:
      fields_copy[db_index] = "IF(Db = DATABASE(), 'DATABASE()', Db)"
    check_query = 'SELECT ORDERED_CHECKSUM(%s) FROM %s WHERE %s;' % (
        ','.join(fields_copy), name, delete_where)
    try:
      dbh.ExecuteOrDie(check_query)
    except db.QueryErrorsException:
      # Google extensions not available, fall back to all shards.
      logging.info('Google SQL extensions not available; pulling data from all '
                   'shards')
      query = 'SELECT %s FROM %s WHERE %s;' % (
          ','.join(fields), name, delete_where)
      old_table = dbh.ExecuteOrDie(query)
      return cls._GetTableDiffSQL(dbh, old_table, table, name)
    except db.InconsistentResponses:
      # Shards differ, fall back to non-incremental.
      logging.info('%s differs across shards; falling back to non-incremental',
                   name)
      return cls._GetFullTableSQL(table, name, delete_where)

    # All shards match
    query = 'ON SHARD 0 SELECT %s FROM %s WHERE %s;' % (
        ','.join(fields), name, delete_where)
    old_table = dbh.ExecuteOrDie(query)
    return cls._GetTableDiffSQL(dbh, old_table, table, name)

  @staticmethod
  def _GetFullTableSQL(table, name, delete_where):
    """Generate SQL to delete and rebuild table contents."""
    if delete_where:
      yield 'DELETE FROM %s WHERE %s;' % (name, delete_where)
    for sql in table.GetInsertSQLList(name, max_size=2**20,
                                      on_duplicate_key_update=True):
      yield sql

  @classmethod
  def GetSQL(cls, dbh, tables, delete_where=None, map=None, incremental=True):
    """Generate an ordered list of SQL statements to push these permissions.

    Args:
      dbh: Database connection handle, used to build diffs against existing
        table contents.
      tables: dict of table name -> VirtualTable instance
      delete_where: a WHERE clause to apply to the DELETE statement; if not
        specified, no DELETEs are emitted.  Specify '1' to delete all rows.
      map: mapping dict from keys in tables to database table names
    """
    sql = []
    for name, table in sorted(tables.iteritems()):
      if map:
        name = map[name]
      if incremental and dbh:
        for row in cls._GetIncrementalTableSQL(dbh, table, name, delete_where):
          yield row
      else:
        for row in cls._GetFullTableSQL(table, name, delete_where):
          yield row

  def ValidateResults(self, dbh, where, map=None):
    if map:
      user_table = map['user']
    else:
      user_table = 'user'

    result = dbh.ExecuteOrDie(
        'SELECT COUNT(*) AS c FROM %s WHERE %s' % (user_table, where))
    # TODO(flamingcow): Compare to previous value to make sure we haven't shrunk
    #   the table too much; do this for all tables.
    if result[0]['c'] == 0:
      dbh.ExecuteOrDie('ROLLBACK;')
      raise ValidationFailure('Operation resulted in blank user list')

  def PrintSets(self):
    """Print the set names available in this PermissionsFile."""
    print '\n'.join(sorted(self.ListSets()))

  def ListSets(self):
    """Return the set names available in this PermissionsFile."""
    return self._sets.keys()

  PUSH_MAP = {
      'columns_priv': 'mysql.columns_priv',
      'db':           'mysql.db',
      'tables_priv':  'mysql.tables_priv',
      'user':         'mysql.user',
      }

  def Push(self, dbh, set_name, incremental=True):
    """Push the permissions defined by set_name to the live database.

    This rewrites the contents of the mysql.* tables for all shards of the
    database represented by dbh, and changes take affect immediately.  Note
    however that changes are _not_ propagated to slaves.

    Args:
      dbh: A connected database handle (pylib.db).
      set_name: The permissions set name (e.g. "primary", "secondary") to apply.
      incremental: Whether the publish should attempt to make minimal changes to
          the destination tables, rather than just wiping them.

    Raises:
      NeedDBAccess: if dbh is None
    """
    if not dbh:
      raise NeedDBAccess
    tables = self.GetSetTables(dbh, set_name)
    for cmd in self.GetSQL(dbh, tables, '1', self.PUSH_MAP,
                           incremental=incremental):
      dbh.ExecuteOrDie(cmd)
    self.ValidateResults(dbh, '1', self.PUSH_MAP)
    dbh.ExecuteOrDie('FLUSH LOCAL PRIVILEGES;')

  PUBLISH_MAP = {
      'columns_priv': 'admin.MysqlPermissionsColumnsPriv',
      'db':           'admin.MysqlPermissionsDb',
      'tables_priv':  'admin.MysqlPermissionsTablesPriv',
      'user':         'admin.MysqlPermissionsUser',
      }

  def Publish(self, dbh, set_name, dest_set_name, push_duration=1800,
              incremental=True, extra_where=None):
    """Publish the permissions defined by set_name to the database.

    This "publishes" the permissions to the the database.  The permissions are
    stored in a set of tables in the 'admin' database using the provided
    dest_set_name.  Permissions can then be applied by use.Copy(), but are
    instead typically applied automatically by Droid.

    Tables published::
      admin.MysqlPermissionsColumnsPriv
      admin.MysqlPermissionsDb
      admin.MysqlPermissionsTablesPriv
      admin.MysqlPermissionsUser

    Args:
      dbh: A connected database handle (pylib.db).
      set_name: The permissions set name to publish.
      dest_set_name: The set name to use in the admin.* tables.
      push_duration: Duration in seconds for the permissions push to last.
          This sets the MysqlPermissionSets.PushDuration column which can be
          used by copiers to decide when to copy.
      incremental: Whether the publish should attempt to make minimal changes to
          the destination tables, rather than just wiping them.
      extra_where: An additional where clause, see below.
    """
    set_id = GetPermissionSetId(dbh, dest_set_name)
    tables = self.GetSetTables(dbh, set_name)
    for table in tables.values():
      table.AddField('MysqlPermissionSetId', set_id)
    with dbh.Transaction():
      where = 'MysqlPermissionSetId=%s' % dbh.Escape(set_id)
      if extra_where:
        where = '(%s AND %s)' % (where, extra_where)
      for cmd in self.GetSQL(dbh, tables, where, self.PUBLISH_MAP,
                             incremental=incremental):
        dbh.ExecuteOrDie(cmd)
      self.ValidateResults(dbh, where, self.PUBLISH_MAP)
      MarkPermissionsChanged(dbh, set_id, int(push_duration))

  def Dump(self, dbh, set_name, user=None, incremental=True):
    """Print the SQL statements comprising the named permissions set.

    Args:
      dbh: A connected database handle (pylib.db).
      set_name: The permissions set name, e.g. "primary", "secondary".
      user: Dump permissions only for the named account.
      incremental: Whether the publish should attempt to make minimal changes to
          the destination tables, rather than just wiping them.
    """
    if user:
      account = self._sets[set_name].GetAccount(user)
      tables = account.GetTables(dbh)
      print '\n'.join(
          self.GetSQL(dbh, tables, None, self.PUSH_MAP,
                      incremental=incremental))
    else:
      tables = self.GetSetTables(dbh, set_name)
      print '\n'.join(
          self.GetSQL(dbh, tables, '1', self.PUSH_MAP,
                      incremental=incremental))

  def Test(self, dbh, incremental=True):
    """Test all defined permissions sets.

    Args:
      dbh: A connected database handle (pylib.db).
      incremental: Whether the publish should attempt to make minimal changes to
          the destination tables, rather than just wiping them.
    """
    for permissions_set in self._sets:
      tables = self.GetSetTables(dbh, permissions_set)
      for _ in self.GetSQL(dbh, tables, '1', self.PUSH_MAP,
                           incremental=incremental):
        pass

  def GetSet(self, set_name):
    """Fetch a set object by name."""
    return self._sets[set_name]
