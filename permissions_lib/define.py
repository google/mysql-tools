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

"""Define MySQL permissions.

This module implements a domain-specific language (in Python) for defining MySQL
permissions.

A permissions file can use this framework by running::

  SetAllowedFields(['ssl_cipher', 'x509_issuer', 'x509_subject'])
  user = Account(username='droid', password_hash='*ABD2189031289051',
                 ssl_cipher='', x509_issuer='', x509_subject='')
  user.AddAllowedHost(hostname_pattern='%')
  user.Export(set='secondary')

Chained method calls may also be used::

  (Account(username='droid', password_hash='*ABD2189031289051',
           ssl_cipher='', x509_issuer='', x509_subject='')
   .AddAllowedHost(hostname_pattern='%')
   .GrantPrivileges(privileges=SUPER)
   .Export(set='secondary')
   .GrantPrivileges(privileges=ALL_PRIVILEGES)
   .Export(set='primary'))

SetAllowedFields() should be called once, before defining any Accounts.

See the documentation for L{Account}.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'
__docformat__ = 'epytext en'

import copy
import functools

try:
  from pylib import db
except ImportError:
  from ..pylib import db

import utils


SETS = {}
"""Dictionary of setname -> Set.

This variable is used during push/publish/dump operations; it should not appear
in a permissions file.
"""


class Error(Exception):
  """The base class for exceptions in this module."""


class DuplicateAccount(Error):
  """Raised when an account is exported to the same set."""


class InvalidPrivileges(Error):
  """Raised when invalid privilege combinations are attempted."""


class InvalidKey(Error):
  """Raised when an unknown field or comment name is used in SetFields."""


class MissingFieldsError(Error):
  """Raised when exporting an account with no username or password."""


class NeedDBAccess(Error):
  """Raised when a database handle is required to build a permission set."""


class NoSuchAccount(Error):
  """Raised when an account name is not found (by FindAccount)."""


class NoSuchSet(Error):
  """Raised when a permissions set is not found (by FindAccount)."""


class DecryptionRequired(Error):
  """Raised when an account that needs decryption is used."""


class DecryptionFailed(Error):
  """Raised when decryption fails, due to invalid key or data."""


_TOTAL_PRIVS = 26
_PRIVS = [2**x for x in xrange(_TOTAL_PRIVS)]
(
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE,
    DROP,
    RELOAD,
    SHUTDOWN,
    PROCESS,
    FILE,
    GRANT,
    REFERENCES,
    INDEX,
    ALTER,
    SHOW_DATABASES,
    SUPER,
    CREATE_TEMP_TABLE,
    LOCK_TABLES,
    EXECUTE,
    REPL_SLAVE,
    REPL_CLIENT,
    CREATE_VIEW,
    SHOW_VIEW,
    CREATE_ROUTINE,
    ALTER_ROUTINE,
    CREATE_USER,
) = _PRIVS


ALL_PRIVILEGES = sum(_PRIVS)
"""All privileges.  See L{Account.GrantPrivileges()}."""


# Using this in place of a database name causes substitution for the "main
# database", e.g. "ads54".
DEFAULT = ('literal', 'DATABASE()')
"""Refers to the (sharded) database name.

This is used to indicate that a permission should be set on the database
specified by the --db flag during a push/publish/dump operation.  See
L{Account.GrantPrivileges}().
"""


_FIELD_NAMES = {
    SELECT:            'Select_priv',
    INSERT:            'Insert_priv',
    UPDATE:            'Update_priv',
    DELETE:            'Delete_priv',
    CREATE:            'Create_priv',
    DROP:              'Drop_priv',
    RELOAD:            'Reload_priv',
    SHUTDOWN:          'Shutdown_priv',
    PROCESS:           'Process_priv',
    FILE:              'File_priv',
    GRANT:             'Grant_priv',
    REFERENCES:        'References_priv',
    INDEX:             'Index_priv',
    ALTER:             'Alter_priv',
    SHOW_DATABASES:    'Show_db_priv',
    SUPER:             'Super_priv',
    CREATE_TEMP_TABLE: 'Create_tmp_table_priv',
    LOCK_TABLES:       'Lock_tables_priv',
    EXECUTE:           'Execute_priv',
    REPL_SLAVE:        'Repl_slave_priv',
    REPL_CLIENT:       'Repl_client_priv',
    CREATE_VIEW:       'Create_view_priv',
    SHOW_VIEW:         'Show_view_priv',
    CREATE_ROUTINE:    'Create_routine_priv',
    ALTER_ROUTINE:     'Alter_routine_priv',
    CREATE_USER:       'Create_user_priv',
}


_FIELD_SET_NAMES = {
    SELECT:            'Select',
    INSERT:            'Insert',
    UPDATE:            'Update',
    DELETE:            'Delete',
    CREATE:            'Create',
    DROP:              'Drop',
    GRANT:             'Grant',
    REFERENCES:        'References',
    INDEX:             'Index',
    ALTER:             'Alter',
    CREATE_VIEW:       'Create View',
    SHOW_VIEW:         'Show view',
}


_ALLOWED_FIELDS = []
_ALLOWED_COMMENTS = []


def SetAllowedFields(fields):
  """Set the list of field names which can be set on an Account.

  Fields are per-account key-value pairs and are exported to the mysql.user
  table.

  Args:
    fields: a sequence of field names which must exist in the target table
  """
  global _ALLOWED_FIELDS
  _ALLOWED_FIELDS = fields


def SetAllowedComments(comments):
  """Set the list of comment names which can be set on an Account.

  Comments are per-account key-value pairs.  They are administrative-only and
  are not exported (they do not affect the users' permissions).

  Args:
    comments: a sequence of comment field names
  """
  global _ALLOWED_COMMENTS
  _ALLOWED_COMMENTS = comments


class _BasePermission(object):
  """Common methods for other permission classes (user, db, etc.)."""
  _valid_privs = 0
  _child_class = None
  _positive_privs = 0
  _negative_privs = 0

  def __init__(self, entity_name=None):
    # Despite the fact that we use it in several functions here, we don't take
    # information on our parents (e.g. _ColumnPermission doesn't know what
    # Table/Database/User it's under) because we have to lazily bind User (due
    # to Clone()), so we might as well lazily bind the rest.
    self._children = {}
    self.SetEntityName(entity_name)

  def SetEntityName(self, entity_name):
    """Set the entity (user, database, table, column) name."""
    self._entity_name = entity_name

  def GetPositivePrivs(self):
    """Get the bitfield of positive privileges."""
    return self._positive_privs

  def GetOrCreateChild(self, child_name):
    """Get a child instance, or create it if it doesn't exist yet.

    Args:
      child_name: The entity_name to create the child with
    """
    if child_name not in self._children:
      self._children[child_name] = self._child_class(child_name)
    return self._children[child_name]

  def GetChildren(self):
    """Retrieve all child objects.

    Returns:
      Dictionary of (database, table, column) name -> permissions object.
    """
    return self._children

  def _CreateAllChildren(self, dbh, fixed_values):
    """Find all possible children (talking to the database) and create them.

    Args:
      dbh: Database handle
      fixed_values: Dictionary of parent entity names
    """
    possible_children = self._child_class.GetAllEntities(dbh, fixed_values)
    for child_name in possible_children:
      self.GetOrCreateChild(child_name)

  def _ValidatePrivileges(self, privs):
    """Check that privs are valid for this permission type."""
    if privs & self._valid_privs != privs:
      raise InvalidPrivileges('Some privileges are not valid for %s' %
                              self.__class__.__name__)

  def GrantPrivileges(self, privs):
    """Grant privileges, overriding negative privileges.

    Args:
      privs: Privileges bitfield.
    """
    self._ValidatePrivileges(privs)
    self._positive_privs |= privs
    self._negative_privs &= ~privs

  def RevokePrivileges(self, privs):
    """Revoke privileges, overriding positive privileges.

    Args:
      privs: Privileges bitfield.
    """
    self._ValidatePrivileges(privs)
    self._negative_privs |= privs
    self._positive_privs &= ~privs

  def PopulateTables(self, tables, fixed_values={}):
    """Populate tables from this instance and all children recursively."""
    self.PopulateTable(tables[self._table_name], fixed_values)
    fixed_values = fixed_values.copy()
    fixed_values[self._entity_field] = self._entity_name
    for child in self._children.values():
      child.PopulateTables(tables, fixed_values)

  def GetNegativePrivsRecursive(self):
    """Get negative privileges from this and all children."""
    negative_privs = self._negative_privs
    for child in self._children.values():
      negative_privs |= child.GetNegativePrivsRecursive()
    # Don't bubble up negative privs that we can satisfy here
    negative_privs &= ~self._positive_privs
    return negative_privs

  def SoftGrantPrivileges(self, privs):
    """Grant a privilege only if we don't have a negative permission for it.

    If we do have a negative permission, remove it, so traversing this tree
    later should yield no negative permissions.
    """
    soft_grant = self._negative_privs & privs
    hard_grant = privs & ~soft_grant
    self._negative_privs &= ~soft_grant
    self._positive_privs |= hard_grant

  def SoftRevokePrivileges(self, privs):
    """Revoke privileges without setting negative permissions for them."""
    self._positive_privs &= ~privs

  def PushDownPrivileges(self, dbh=None, fixed_values={}):
    """Remove negative privileges by pushing down positive privs.

    Check for negative permissions from all descendants.  Push down any negative
    privs to immediate children.  Ask children to do the same.  This ends in a
    state where there are no negative privs left (but many more positive privs
    at leaves).
    """
    fixed_values = fixed_values.copy()
    fixed_values[self._entity_field] = self._entity_name

    descendant_negative_privs = 0
    for child in self._children.values():
      descendant_negative_privs |= child.GetNegativePrivsRecursive()

    push_down = descendant_negative_privs & self._positive_privs
    if push_down:
      self._CreateAllChildren(dbh, fixed_values)
      self.SoftRevokePrivileges(push_down)
      for child in self._children.itervalues():
        child.SoftGrantPrivileges(push_down)

    for child in self._children.values():
      child.PushDownPrivileges(dbh, fixed_values)

  def PrivilegesForPullUp(self):
    """Get privileges that could be pulled up from this child."""
    return self._positive_privs & ~self._negative_privs

  def IsEmpty(self):
    """Check if this child contains no useful content."""
    return not(self._negative_privs or self._positive_privs or self._children)

  def PullUpPrivileges(self, inherited=0):
    """Remove spurious positive privileges from children.

    If children have no negative privileges and have positive privileges
    completely covered by my positive privileges, then the children are
    meaningless: remove them.
    """
    inherited |= self._positive_privs
    for name, child in self._children.items():
      child.PullUpPrivileges(inherited)
      privs = child.PrivilegesForPullUp()
      child.SoftRevokePrivileges(inherited & privs)
      if child.IsEmpty():
        del self._children[name]


class _ColumnPrivsMixIn(object):
  """Methods for permissions tables that store perms in separate columns."""

  _skip_row_without_privs = True

  @classmethod
  def BuildTable(cls):
    fields = list(cls._fixed_fields)
    for priv in _PRIVS:
      if priv & cls._valid_privs:
        fields.append(_FIELD_NAMES[priv])
    return db.VirtualTable(fields, [])

  def PopulateTable(self, table, fixed_values={}):
    if self._skip_row_without_privs and not self._positive_privs:
      return

    values = []
    for field in self._fixed_fields:
      if field == self._entity_field:
        values.append(self._entity_name)
      else:
        # If this throws KeyError, fixed_values is incomplete
        values.append(fixed_values[field])
    for priv in _PRIVS:
      if not (priv & self._valid_privs):
        continue
      if priv & self._positive_privs:
        values.append('Y')
      else:
        values.append('N')
    table.Append(values)


class _SetPrivsMixIn(object):
  """Methods for permissions tables that store permissions in sets."""

  def _PrivsToString(self, privs):
    set_entries = []
    for priv in _PRIVS:
      if priv & privs:
        set_entries.append(_FIELD_SET_NAMES[priv])
    return ','.join(set_entries)


class _ColumnPermission(_BasePermission, _SetPrivsMixIn):
  _valid_privs = (SELECT | INSERT | UPDATE | REFERENCES)
  _table_name = 'columns_priv'
  _entity_field = 'Column_name'

  @classmethod
  def BuildTable(cls):
    return db.VirtualTable(('Host', 'Db', 'User', 'Table_name', 'Column_name',
                            'Column_priv'), [])

  def PopulateTable(self, table, fixed_values):
    if not self._positive_privs:
      return
    values = [
        fixed_values['Host'],
        fixed_values['Db'],
        fixed_values['User'],
        fixed_values['Table_name'],
        self._entity_name,
        self._PrivsToString(self._positive_privs),
    ]
    table.Append(values)

  @staticmethod
  def GetAllEntities(dbh, fixed_values={}):
    if not dbh:
      raise NeedDBAccess('Need to retrieve column list from %s.%s' %
                         (str(fixed_values['Db']), fixed_values['Table_name']))
    if fixed_values['Db'] == DEFAULT:
      result = dbh.CachedExecuteOrDie('SHOW FIELDS FROM `%s`' %
                                      fixed_values['Table_name'])
    else:
      result = dbh.CachedExecuteOrDie('SHOW FIELDS FROM `%s`.`%s`' %
                                      (fixed_values['Db'],
                                       fixed_values['Table_name']))
    return [row['Field'] for row in result]


class _TablePermission(_BasePermission, _SetPrivsMixIn):
  _valid_privs = (SELECT | INSERT | UPDATE | DELETE | CREATE | DROP | GRANT |
                  REFERENCES | INDEX | ALTER | CREATE_VIEW | SHOW_VIEW)
  _child_class = _ColumnPermission
  _table_name = 'tables_priv'
  _entity_field = 'Table_name'

  @classmethod
  def BuildTable(cls):
    # Column_priv in tables_priv tells MySQL to check the columns_priv table
    # and allow it to grant just that set of permissions.  It therefore has to
    # be the bitwise OR of all positive column permissions under it.  Yes, this
    # is stupid.
    return db.VirtualTable(('Host', 'Db', 'User', 'Table_name', 'Table_priv',
                            'Column_priv'), [])

  def PopulateTable(self, table, fixed_values):
    column_privs = 0
    for child in self._children.itervalues():
      column_privs |= child.GetPositivePrivs()
    if not self._positive_privs and not column_privs:
      return
    if column_privs & self._positive_privs:
      raise InvalidPrivileges(
          'Duplication between table and column privileges for %s (%s): '
          '(%s)/(%s)' % (
              self._entity_name,
              fixed_values,
              self._PrivsToString(self._positive_privs),
              self._PrivsToString(column_privs)))
    values = [
        fixed_values['Host'],
        fixed_values['Db'],
        fixed_values['User'],
        self._entity_name,
        self._PrivsToString(self._positive_privs),
        self._PrivsToString(column_privs),
    ]
    table.Append(values)

  @staticmethod
  def GetAllEntities(dbh, fixed_values={}):
    if not dbh:
      raise NeedDBAccess('Need to retrieve table list from %s' %
                         str(fixed_values['Db']))
    if fixed_values['Db'] == DEFAULT:
      dbname = 'DATABASE()'
    else:
      dbname = "'%s'" % fixed_values['Db'].replace("'", "''")
    result = dbh.CachedExecuteOrDie(
        'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE'
        ' TABLE_SCHEMA=%s' % dbname)
    return [row['TABLE_NAME'] for row in result]


class _DatabasePermission(_BasePermission, _ColumnPrivsMixIn):
  _valid_privs = (SELECT | INSERT | UPDATE | DELETE | CREATE | DROP | GRANT |
                  REFERENCES | INDEX | ALTER | CREATE_TEMP_TABLE |
                  LOCK_TABLES | CREATE_VIEW | SHOW_VIEW | CREATE_ROUTINE |
                  ALTER_ROUTINE | EXECUTE)
  _child_class = _TablePermission
  _fixed_fields = ['Host', 'Db', 'User']
  _table_name = 'db'
  _entity_field = 'Db'

  @staticmethod
  def GetAllEntities(dbh, fixed_values={}):
    if not dbh:
      raise NeedDBAccess('Need to retrieve database list')
    result = dbh.CachedExecuteOrDie(
        'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE'
        ' SCHEMA_NAME != DATABASE()')
    return [row['SCHEMA_NAME'] for row in result] + [DEFAULT]


class _UserPermission(_BasePermission, _ColumnPrivsMixIn):
  _valid_privs = (SELECT | INSERT | UPDATE | DELETE | CREATE| DROP | RELOAD |
                  SHUTDOWN | PROCESS | FILE | GRANT | REFERENCES | INDEX |
                  ALTER | SHOW_DATABASES | SUPER | CREATE_TEMP_TABLE |
                  LOCK_TABLES | EXECUTE | REPL_SLAVE | REPL_CLIENT |
                  CREATE_VIEW | SHOW_VIEW | CREATE_ROUTINE | ALTER_ROUTINE |
                  CREATE_USER)
  _child_class = _DatabasePermission
  _fixed_fields = ['Host', 'User', 'Password']
  _table_name = 'user'
  _entity_field = 'User'
  _skip_row_without_privs = False


def _KeywordArgumentsOnly(func):
  """Decorator that only allows keyword, not positional, arguments."""

  def PleaseUseKeywordArguments(self, **kwargs):
    return func(self, **kwargs)
  PleaseUseKeywordArguments.__doc__ = func.__doc__
  return PleaseUseKeywordArguments


def _KeywordArgumentsOnlyGlobal(func):
  """Like KeywordArguments, but for global functions."""

  def PleaseUseKeywordArguments(**kwargs):
    return func(**kwargs)
  PleaseUseKeywordArguments.__doc__ = func.__doc__
  return PleaseUseKeywordArguments


class Account(object):
  """A database account or account template.

  Includes both authentication and authorization data.
  """
  @_KeywordArgumentsOnly
  def __init__(self, username=None, password_hash=None, password=None,
               encrypted_hash=None, **kwargs):
    """Creates a new account instance.

    An Account can represent an actual exported account and/or a template.  Note
    that creating an account does not cause it to appear in permissions output;
    Export() must be called at least once for this to happen.

    In MySQL 5, three keyword arguments are required to prevent db.py
    from throwing a QueryWarningsException:
        ssl_cipher='', x509_issuer='', x509_subject=''
    For this to work, call
        SetAllowedFields(['ssl_cipher', 'x509_issuer', 'x509_subject'])
    before instantiating any Accounts.

    All other arguments are optional and must be keywords; see InitUser() and
    SetFields() for available arguments.

    Args: see InitUser() and SetFields().
    """
    self._allowed_hosts = set()
    self._extra_fields = {}
    self._comments = {}
    self._username = None
    self._password_hash = None
    self._encrypted_hash = None
    # For each privilege set, we store positive and negative privilege bits.
    # Privileges are defined as (positive & ~negative).  More specific
    # privileges override less-specific ones.  As MySQL only supports positive
    # permissions, the representation of this in SQL may be significantly more
    # complicated than here.
    self._perm = _UserPermission(username)
    self.InitUser(username=username,
                  password_hash=password_hash,
                  password=password,
                  encrypted_hash=encrypted_hash)
    self.SetFields(**kwargs)

  @_KeywordArgumentsOnly
  def InitUser(self, username=None, password_hash=None, password=None,
               encrypted_hash=None):
    """Handle constructor or copy user/password initialization.

    Any argument may be None, meaning "do not change".

    Args:
      username: the string that the user will supply as a username when
          connecting to MySQL
      password: the cleartext password for this account.  Using password_hash
          instead is strongly recommended, in which case this is ignored.
      password_hash: the double-SHA1 hash of this user's password, as returned
          by the MySQL (version 4.1 and higher) PASSWORD() command
      encrypted_hash: the hash as above which was then encrypted with an RSA
          public key and encoded into base64.

    Returns:
      self (allows method call chaining)
    """
    if username is not None:
      self._username = username
      self._perm.SetEntityName(username)
    if password_hash is not None:
      self._password_hash = password_hash
    elif password is not None:
      self._password_hash = utils.HashPassword(password)
    elif encrypted_hash is not None:
      self._password_hash = None  # Set by Decrypt()
      self._encrypted_hash = encrypted_hash
    return self

  @_KeywordArgumentsOnly
  def Clone(self, username=None, password_hash=None, password=None,
            encrypted_hash=None, **kwargs):
    """Clone an Account.

    The username or password may be changed here as well.

    Args: see InitUser() and SetFields()

    Returns:
      a new Account
    """
    new = copy.deepcopy(self)
    new.InitUser(username=username,
                 password_hash=password_hash,
                 password=password,
                 encrypted_hash=encrypted_hash)
    new.SetFields(**kwargs)
    return new

  @_KeywordArgumentsOnly
  def AddAllowedHost(self, hostname_pattern):
    """Add a hostname pattern from which the account is allowed to connect.

    All privileges are granted equally to all hostname patterns; while this is
    not a MySQL restriction, it is enforced here for sanity.

    Args:
      hostname_pattern: an IP address, hostname, or pattern
          This can only be a hostname or hostname pattern if the MySQL servers
          in question have reverse DNS resolution enabled (not recommended).
          Examples:
            All hosts: %
            IP address: 1.2.3.4
            IP address pattern: 1.2.3.%
            IP and netmask: 1.2.3.0/255.255.255.128

    Returns:
      self (allows method call chaining)
    """

    self._allowed_hosts.add(hostname_pattern)
    return self

  def ClearAllowedHosts(self):
    """Clear all hostname patterns previously added to this account.

    Returns:
      self (allows method call chaining)
    """
    self._allowed_hosts.clear()
    return self

  def _FindEntity(self, database, table, column):
    if not database:
      return self._perm
    database_perm = self._perm.GetOrCreateChild(database)

    if not table:
      return database_perm
    table_perm = database_perm.GetOrCreateChild(table)

    if not column:
      return table_perm
    column_perm = table_perm.GetOrCreateChild(column)

    return column_perm

  @_KeywordArgumentsOnly
  def GrantPrivileges(self, database=None, table=None, column=None,
                      privileges=0):
    """Grant privileges at the user, database, table or column level.

    The DB permissions system supports granting privileges at a wide level, e.g.
    database, then revoking them at a specific level (e.g. column).  Positive
    privileges are propagated down properly.

    Calling GrantPrivileges without setting 'database', 'table' or 'column'
    grants privileges at the user level.

    Args:
      database: database name on which to grant privileges
          The special constant DEFAULT is defined and indicates that the
          permissions should be granted on the database passed with the --db
          flag during the push/publish/dump operation.  This is useful for
          sharded databases, where the database may be named differently per
          shard.
          If not set, privileges are granted at the user level.
      table: table name on which to grant privileges
          If set, database must also be set.
      column: column name on which to grant privileges
          If set, database and table must also be set.
      privileges: a bit mask of privileges to grant, created by ORing
          together (using the | operator) the following privileges:
            SELECT
            INSERT
            UPDATE
            DELETE
            CREATE
            DROP
            RELOAD
            SHUTDOWN
            PROCESS
            FILE
            GRANT
            REFERENCES
            INDEX
            ALTER
            SHOW_DATABASES
            SUPER
            CREATE_TEMP_TABLE
            LOCK_TABLES
            EXECUTE
            REPL_SLAVE
            REPL_CLIENT
            CREATE_VIEW
            SHOW_VIEW
            CREATE_ROUTINE
            ALTER_ROUTINE
            CREATE_USER
          The meta-privilege ALL_PRIVILEGES is also available.
          Not all privileges are valid at all levels.
          For details, see:
            http://dev.mysql.com/doc/refman/5.0/en/privileges-provided.html

    Returns:
      self (allows method call chaining)
    """

    self._FindEntity(database, table, column).GrantPrivileges(privileges)
    return self

  @_KeywordArgumentsOnly
  def RevokePrivileges(self, database=None, table=None, column=None,
                       privileges=0):
    """Revoke privileges at the user, database, column, or table level.

    See GrantPrivileges().

    Calling RevokePrivileges() with the same options as a previous
    GrantPrivileges() reverses the effects of the previous call, but
    additionally overrides any redundant inherited permissions.

    Args: see GrantPrivileges()

    Returns:
      self (allows method call chaining)
    """
    self._FindEntity(database, table, column).RevokePrivileges(privileges)
    return self

  def SetFields(self, **kwargs):
    """Set key/value data that is exported to the user table or comments.

    The keys must previously have been defined with either SetAllowedFields() or
    SetAllowedComments().  The function is called with the key/value pairs as
    named arguments, e.g. SetFields(max_connections=10).  Fields supported by
    the Ads MySQL build at the time of writing:

      ssl_type
      ssl_cipher (required)
      x509_issuer (required)
      x509_subject (required)
      max_questions
      max_updates
      max_connections
      max_user_connections

    Raises:
      InvalidKey: Raised when a parameter has not been defined with
          SetAllowedFields() or SetAllowedComments().

    Returns:
      self (allows method call chaining)
    """
    for key, value in kwargs.iteritems():
      if key in _ALLOWED_FIELDS:
        self._extra_fields[key] = value
      elif key in _ALLOWED_COMMENTS:
        self._comments[key] = value
      else:
        raise InvalidKey('%s is not a valid field name' % key)
    return self

  @_KeywordArgumentsOnly
  def Export(self, set_name):
    """Export the Account to a permissions set.

    This marks the account to be pushed during push/publish/dump operations.

    An account may be exported more than once, to different sets.  A single
    permissions file can contain multiple permissions sets.  A set represents a
    group of accounts and their privileges that is pushed to a host and controls
    all access to that host; only one set can be in effect on a host at any
    given time.

    Note that a copy of the Account is made for each call to Export().  Changes
    made to the Account afterward will not affect any previous Export() calls,
    but will affect future Export() calls.

    Args:
      set_name: the set name (e.g. "primary", "secondary") to which to export
          this account

    Raises:
      MissingFieldsError: Raised when username or password are missing.
      DuplicateAccount: Raised if this account has already been exported to the
          set_name.

    Returns:
      self (allows method call chaining)
    """
    if (self._username is None or (self._password_hash is None and
                                   self._encrypted_hash is None)):
      raise MissingFieldsError(
          'Username and password/password hash/encrypted hash are required to '
          'Export a user')
    set_obj = SETS.setdefault(set_name, Set())
    set_obj.AddAccount(self)
    return self

  def Decrypt(self, key):
    """Decrypt an encrypted hash for this user, if there is one."""
    if self._encrypted_hash is not None and self._password_hash is None:
      self._password_hash = utils.DecryptHash(key, self._encrypted_hash)
      if not self._password_hash:
        raise DecryptionFailed(self.GetUsername())

  def GetUsername(self):
    """Return the username for this account.

    This method is used during push/publish/dump operations and calls to it
    should not appear in a permissions file.

    Returns:
      The username of the account.
    """
    return self._username

  def GetUserPermission(self):
    """Fetch the UserPermission object behind this account."""
    return self._perm

  def GetTables(self, dbh=None):
    """Generate tables containing privilege data for this user.

    This method is used during push/publish/dump operations and calls to it
    should not appear in a permissions file.

    Args:
      dbh: Database handle. Only used here for read-only, schema-description
        queries.

    Returns:
      A dictionary where the keys are a table name in the mysql database and
      the value is db.VirtualTable containing the output rows.

    Raises:
      NeedDBAccess
      DecryptionRequired
    """
    if self._encrypted_hash is not None and self._password_hash is None:
      raise DecryptionRequired(self.GetUsername())

    self._perm.PullUpPrivileges()
    self._perm.PushDownPrivileges(dbh)
    self._perm.PullUpPrivileges()

    ret = {
        'user': _UserPermission.BuildTable(),
        'db': _DatabasePermission.BuildTable(),
        'tables_priv': _TablePermission.BuildTable(),
        'columns_priv': _ColumnPermission.BuildTable(),
        }
    fixed_values = {
        'Password': self._password_hash,
        }

    for host in sorted(self._allowed_hosts):
      fixed_values['Host'] = host
      self._perm.PopulateTables(ret, fixed_values)

    for key in sorted(self._extra_fields.iterkeys()):
      ret['user'].AddField(key, str(self._extra_fields[key]))

    if self._password_hash is None:
      del ret['user']

    return ret


class Set(object):
  """A set of accounts with permissions.

  A permissions file can contain multiple permissions sets. A set is a named
  group of permissions that are active together at a given time. The name to set
  mapping is kept in the global SETS variable.
  """

  def __init__(self):
    # username -> Account
    self._accounts = {}

  def GetTables(self, dbh):
    """Generate a table set for each account, then merge them all together.

    Args:
      dbh: Database handle. Only used here for read-only, schema-description
        queries.

    Returns:
      A dictionary where the keys are a table name in the mysql database and
      the value is db.VirtualTable containing the output rows.
    """
    tables = {}
    for account in self._accounts.itervalues():
      # We use a temporary account copy because GetAccountTables() alters the
      # account object, expanding out the privileges. This can result in a much
      # larger object, so we don't want to keep it around.
      temp_account = account.Clone()
      acct_tables = temp_account.GetTables(dbh)
      for table, result in acct_tables.iteritems():
        if table in tables:
          tables[table].Merge(result)
        else:
          tables[table] = result
    return tables

  def AddAccount(self, account):
    if account.GetUsername() in self._accounts:
      raise DuplicateAccount(account.GetUsername())
    self._accounts[account.GetUsername()] = account.Clone()

  def SetAllFields(self, **kwargs):
    # Split out fields from username/password/password_hash
    fields = kwargs.copy()
    if 'username' in fields: del fields['username']
    if 'password' in fields: del fields['password']
    if 'password_hash' in fields: del fields['password_hash']
    if 'encrypted_hash' in fields: del fields['encrypted_hash']
    for account in self._accounts.itervalues():
      account.InitUser(username=kwargs.get('username', None),
                       password=kwargs.get('password', None),
                       password_hash=kwargs.get('password_hash', None),
                       encrypted_hash=kwargs.get('encrypted_hash', None))
      account.SetFields(**fields)

  def SetAllAllowedHosts(self, hostname_patterns):
    for account in self._accounts.itervalues():
      account.ClearAllowedHosts()
      for hostname_pattern in hostname_patterns:
        account.AddAllowedHost(hostname_pattern=hostname_pattern)

  def GetAccount(self, username):
    return self._accounts[username]

  def GetAccounts(self):
    return self._accounts.copy()

  def Clone(self):
    new_set = Set()
    for account in self._accounts.itervalues():
      new_set.AddAccount(account)
    return new_set

  def Decrypt(self, key):
    """Decrypt hashes for all accounts in this set."""
    for account in self._accounts.itervalues():
      account.Decrypt(key)


# For use by code defining permissions:
@_KeywordArgumentsOnlyGlobal
def FindAccount(set_name, username):
  """Retrieve a previously-created account object."""
  try:
    set = SETS[set_name]
  except KeyError:
    raise NoSuchSet('Set %s has no exported accounts' % set_name)
  try:
    return set.GetAccount(username)
  except KeyError:
    raise NoSuchAccount('No account with username %s in set %s' % (
        username, set_name))


@_KeywordArgumentsOnlyGlobal
def DuplicateSet(source_set_name, destination_set_name):
  """Create a new set that contains all accounts in an existing set."""
  SETS[destination_set_name] = SETS[source_set_name].Clone()


@_KeywordArgumentsOnlyGlobal
def SetAllFields(set_name, **kwargs):
  """Set field values on every account in a set."""
  SETS[set_name].SetAllFields(**kwargs)


@_KeywordArgumentsOnlyGlobal
def SetAllAllowedHosts(set_name, hostname_patterns):
  """Set allowed host patterns on every account in a set."""
  SETS[set_name].SetAllAllowedHosts(hostname_patterns)
