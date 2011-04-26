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


def Copy(dbh, set_name):
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
  """
  dbh.ExecuteOrDie('BEGIN')
  # Mutex locking against parallel copies.
  dbh.ExecuteOrDie(
      'SELECT 1 FROM adminlocal.LocalMysqlPermissionState FOR UPDATE')
  try:
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
    if not result[0]['c']:
      logging.fatal('Permissions copy with blank user table; aborting.')

    for dest, source in COPY_MAP.iteritems():
      result = dbh.ExecuteOrDie('DESC %s' % dest)
      columns = [row['Field'] for row in result]
      columns_str = ','.join(columns)
      dbh.ExecuteOrDie('DELETE FROM %s' % dest)
      dbh.ExecuteOrDie('INSERT INTO %s SELECT %s FROM %s '
                       ' WHERE MysqlPermissionSetId=%s' % (
                           dest, columns_str, source, set_id))

    dbh.ExecuteOrDie('DELETE FROM adminlocal.LocalMysqlPermissionState')
    dbh.ExecuteOrDie('INSERT INTO adminlocal.LocalMysqlPermissionState SET'
                     ' MysqlPermissionSetId=%(set_id)s, '
                     ' LastPushed=FROM_UNIXTIME(%(last_pushed)s)',
                     {'set_id': set_id,
                      'last_pushed': last_updated})
    dbh.ExecuteOrDie('COMMIT')
    dbh.ExecuteOrDie('FLUSH LOCAL PRIVILEGES')
  finally:
    dbh.ExecuteOrDie('ROLLBACK')


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

  def __init__(self, permissions_contents, private_keyfile=None):
    """Load the permissions definitions.

    TODO(flamingcow): There is a race when instantiating two instances of
    PermissionsFile at once. We need a better way to return values from
    define.py than a global.

    Args:
      permissions_contents: string containing Python code that defines
        permissions, using the framework provided in define.py.
      private_keyfile: Path to a file containing the private RSA key to be
        used to decrypt encrypted_hash values from permissions_contents.
    """
    self.globals = define.__dict__
    # Reset define's global state. This makes multiple instantiations of
    # PermissionsFile work.
    self.globals['SETS'].clear()
    code = compile(permissions_contents, 'permissions contents', 'exec')
    exec(code, self.globals)
    self._sets = self.globals['SETS'].copy()
    if private_keyfile:
      self._decryption_key = utils.PrivateKeyFromFile(private_keyfile)
    else:
      self._decryption_key = None

  @staticmethod
  def GetAccountTables(dbh, account):
    """Generate a table set for a single account."""
    if dbh:
      callback = dbh.CachedExecuteOrDie
    else:
      callback = None
    return account.GetTables(query_callback=callback)

  def GetSetTables(self, dbh, set_name):
    """Generate a table set for each account, then merge them all together."""
    if dbh:
      callback = dbh.CachedExecuteOrDie
    else:
      callback = None
    if self._decryption_key:
      self._sets[set_name].Decrypt(self._decryption_key)
    return self._sets[set_name].GetTables(callback)

  @staticmethod
  def GetSQL(tables, delete_where=None, map=None, extended_insert=True):
    """Generate an ordered list of SQL statements to push these permissions.

    Args:
      tables: dict of table name -> VirtualTable instance
      delete_where: a WHERE clause to apply to the DELETE statement; if not
        specified, no DELETEs are emitted.  Specify '1' to delete all rows.
      map: mapping dict from keys in tables to database table names
      extended_insert: If false, one INSERT row per statement.
    """
    sql = []
    for name, table in tables.iteritems():
      if map:
        name = map[name]
      if delete_where:
        yield 'DELETE FROM %s WHERE %s;' % (name, delete_where)
      for sql in table.GetInsertSQLList(name, max_size=2**20,
                                        extended_insert=extended_insert):
        yield sql

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

  def Push(self, dbh, set_name):
    """Push the permissions defined by set_name to the live database.

    This rewrites the contents of the mysql.* tables for all shards of the
    database represented by dbh, and changes take affect immediately.  Note
    however that changes are _not_ propagated to slaves.

    Args:
      dbh: A connected database handle (pylib.db).
      set_name: The permissions set name (e.g. "primary", "secondary") to apply.

    Raises:
      NeedDBAccess: if dbh is None
    """
    if not dbh:
      raise NeedDBAccess
    tables = self.GetSetTables(dbh, set_name)
    for cmd in self.GetSQL(tables, '1', self.PUSH_MAP):
      dbh.ExecuteOrDie(cmd)
    self.ValidateResults(dbh, '1', self.PUSH_MAP)
    dbh.ExecuteOrDie('FLUSH LOCAL PRIVILEGES;')

  PUBLISH_MAP = {
      'columns_priv': 'admin.MysqlPermissionsColumnsPriv',
      'db':           'admin.MysqlPermissionsDb',
      'tables_priv':  'admin.MysqlPermissionsTablesPriv',
      'user':         'admin.MysqlPermissionsUser',
      }

  def Publish(self, dbh, set_name, dest_set_name, push_duration=1800):
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
    """
    set_id = GetPermissionSetId(dbh, dest_set_name)
    tables = self.GetSetTables(dbh, set_name)
    for table in tables.values():
      table.AddField('MysqlPermissionSetId', set_id)
    dbh.ExecuteOrDie('BEGIN;')
    where = 'MysqlPermissionSetId=%s' % dbh.Escape(set_id)
    for cmd in self.GetSQL(tables, where, self.PUBLISH_MAP):
      dbh.ExecuteOrDie(cmd)
    self.ValidateResults(dbh, where, self.PUBLISH_MAP)
    MarkPermissionsChanged(dbh, set_id, int(push_duration))
    dbh.ExecuteOrDie('COMMIT;')

  def Dump(self, dbh, set_name, user=None):
    """Print the SQL statements comprising the named permissions set.

    Args:
      dbh: A connected database handle (pylib.db).
      set_name: The permissions set name, e.g. "primary", "secondary".
      user: Dump permissions only for the named account.
    """
    if user:
      account = self._sets[set_name].GetAccount(user)
      tables = self.GetAccountTables(dbh, account)
      print '\n'.join(self.GetSQL(tables, None, self.PUSH_MAP))
    else:
      tables = self.GetSetTables(dbh, set_name)
      print '\n'.join(self.GetSQL(tables, '1', self.PUSH_MAP))

  def Test(self, dbh):
    """Test all defined permissions sets.

    Args:
      dbh: A connected database handle (pylib.db).
    """
    for set in self._sets:
      tables = self.GetSetTables(dbh, set)
      for _ in self.GetSQL(tables, '1'):
        pass
