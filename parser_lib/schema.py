#!/usr/bin/python2.6
# vim: set fileencoding=utf8 :
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

"""Definitions for table and database schema.

Classes that store schema objects (Database, Table and Column). Schema is
initially loaded from the database, but is then mutable to support simulation of
changes.
"""

# based on the work of Matthieu Tourne (matthieu.tourne@gmail.com)
__author__ = 'flamingcow@google.com (Ian Gulliver)'


class Error(Exception):
  pass


class DuplicateNameException(Error):
  pass


class UnknownNameException(Error):
  pass


class Database(object):
  """A single database (set of tables) in the schema."""

  _data_set = False

  def __init__(self, dbh, db_name):
    self._dbh = dbh
    self._name = db_name
    # Table names are case-sensitive
    self._tables = {}

  def __str__(self):
    if self._name:
      return '`%s`' % self._name
    else:
      return ''

  def _LazyInit(self):
    """Pull the DB information about this object. Delay this if possible."""
    if self._data_set:
      return
    self._data_set = True

    if self._name:
      db = self._dbh.Escape(self._name)
    else:
      db = 'DATABASE()'
    rows = self._dbh.ExecuteOrDie(
        'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE '
        'TABLE_SCHEMA=%s' % db)
    for row in rows:
      table_name = row['TABLE_NAME']
      self._tables[table_name] = Table(self._dbh, self, table_name)

  def AddTable(self, table_name):
    self._LazyInit()
    if table_name in self._tables:
      raise DuplicateNameException('Table `%s` in %s' % (table_name, self))
    self._tables[table_name] = Table(self._dbh, self, table_name, True)
    return self._tables[table_name]

  def DropTable(self, table_name):
    self.FindTable(table_name)
    del self._tables[table_name]

  def FindTable(self, table_name):
    self._LazyInit()
    if table_name not in self._tables:
      raise UnknownNameException('Table `%s` in %s' % (table_name, self))
    return self._tables[table_name]



class Table(object):
  """A single table (list of columns, rows, storage engine) in the schema."""

  _engine = None
  _rows = None
  _data_set = False

  def __init__(self, dbh, database, table_name, new=False):
    self._dbh = dbh
    self._database = database
    self._name = table_name
    # Column names are case-insensitive
    self._columns = {}
    if new:
      self._data_set = True

  def __str__(self):
    db = str(self._database)
    if db:
      return '%s.`%s`' % (db, self._name)
    else:
      return '`%s`' % self._name

  def _LazyInit(self):
    """Pull the DB information about this object. Delay this if possible."""
    if self._data_set:
      return
    self._data_set = True

    rows = self._dbh.ExecuteOrDie('SHOW COLUMNS FROM %s' % self)
    for row in rows:
      self.AddColumn(row['Field'], row['Type'])
    db = str(self._database)
    if db:
      prefix = 'SHOW TABLE STATUS FROM %s' % db
    else:
      prefix = 'SHOW TABLE STATUS'
    rows = self._dbh.ExecuteMerged(prefix + ' WHERE Name=%(name)s',
                                   { 'name': self._name })
    engines = set(row['Engine'] for row in rows)
    assert len(engines) == 1, (
        'Engine for table %s differs between shards' % self)
    self._engine = engines.pop()
    self._rows = max(int(row['Rows']) for row in rows)

  def AddColumn(self, column_name, column_type):
    self._LazyInit()
    column_name = column_name.lower()
    if column_name in self._columns:
      raise DuplicateNameException('Column `%s` in %s' % (column_name, self))
    self._columns[column_name] = Column(self._dbh, self,
                                        column_name, column_type)
    return self._columns[column_name]

  def DropColumn(self, column_name):
    column_name = column_name.lower()
    self.FindColumn(column_name)
    del self._columns[column_name]

  def FindColumn(self, column_name):
    self._LazyInit()
    column_name = column_name.lower()
    if column_name not in self._columns:
      raise UnknownNameException('Column `%s` in %s' % (column_name, self))
    return self._columns[column_name]

  def GetColumns(self):
    self._LazyInit()
    return dict(self._columns)

  def GetEngine(self):
    self._LazyInit()
    return self._engine

  def SetEngine(self, engine):
    self._engine = engine

  def GetRows(self):
    self._LazyInit()
    return self._rows


class Column(object):
  """A single column (with data type) in the schema."""

  def __init__(self, dbh, table, column_name, column_type):
    self._dbh = dbh
    self._table = table
    self._name = column_name
    self._type = column_type

  def __str__(self):
    return '%s.`%s`' % (self._table, self._name)

  def GetType(self):
    return self._type


class Schema(object):
  """A representation of important data about the in-database schema.

  Schema
  ↳ Database
    ↳ Table
      ↳ Column

  Data is loaded lazily from the database handle provided. The in-memory
  schema representation is mutable to support simulation of changes.
  """

  _default_table_type = None
  _data_set = False

  def __init__(self, dbh=None):
    self._dbh = dbh
    # Database names are case-sensitive
    self._databases = {}
    self.AddDatabase(None)

  def _LazyInit(self):
    """Pull the DB information about this object. Delay this if possible."""
    if self._data_set:
      return
    self._data_set = True

    rows = self._dbh.ExecuteOrDie(
        'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA'
        ' WHERE SCHEMA_NAME != DATABASE()')
    for row in rows:
      self.AddDatabase(row['SCHEMA_NAME'])

  def AddDatabase(self, db_name):
    self._LazyInit()
    if db_name in self._databases:
      raise DuplicateNameException('Database `%s`' % db_name)
    self._databases[db_name] = Database(self._dbh, db_name)
    return self._databases[db_name]

  def DropDatabase(self, db_name):
    self.FindDatabase(db_name)
    del self._databases[db_name]

  def FindDatabase(self, db_name):
    self._LazyInit()
    if db_name not in self._databases:
      raise UnknownNameException('Database `%s`' % db_name)
    return self._databases[db_name]

  def FindDatabaseFromSpec(self, spec):
    try:
      return self.FindDatabase(spec['database'][0])
    except KeyError:
      return self.FindDatabase(None)

  def FindTableFromSpec(self, spec, alias_map={}):
    table_name = spec['table'][0]
    if table_name in alias_map:
      spec = alias_map[table_name]
    return self.FindDatabaseFromSpec(spec).FindTable(spec['table'][0])

  def FindColumnFromSpec(self, spec, alias_map={}):
    return self.FindTableFromSpec(spec, alias_map).FindColumn(spec['column'][0])

  def GetDefaultEngine(self):
    """Find the database's default engine type."""
    if not self._default_table_type:
      result = self._dbh.ExecuteOrDie("SHOW VARIABLES LIKE 'table_type'")
      self._default_table_type = result[0]['Value']
    return self._default_table_type
