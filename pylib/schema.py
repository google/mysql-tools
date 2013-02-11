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


class Schema(object):
  """A representation of important data about the in-database schema.

  Schema
  - Database
    - Table
      - Column

  Data is loaded lazily from the database handle provided. The in-memory
  schema representation is mutable to support simulation of changes.
  """

  _default_table_type = None
  _default_character_set = None
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

    rows = self._dbh.ExecuteWithRetry(
        'SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME '
        'FROM INFORMATION_SCHEMA.SCHEMATA '
        'WHERE SCHEMA_NAME != DATABASE()')
    for row in rows:
      self.AddDatabase(row['SCHEMA_NAME'], row['DEFAULT_CHARACTER_SET_NAME'])

  def AddDatabase(self, db_name, char_set=None):
    self._LazyInit()
    if db_name in self._databases:
      raise DuplicateNameException('Database `%s`' % db_name)
    char_set = char_set or self.GetCharacterSet()
    self._databases[db_name] = Database(self._dbh, db_name, char_set)
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
      result = self._dbh.ExecuteWithRetry("SHOW VARIABLES LIKE 'table_type'")
      self._default_table_type = result[0]['Value']
    return self._default_table_type

  def GetCharacterSet(self):
    """Find the database's (server's) character set."""
    if not self._default_character_set:
      res = self._dbh.ExecuteWithRetry(
        "SHOW VARIABLES LIKE 'character_set_server'")
      self._default_character_set = res[0]['Value']
    return self._default_character_set


class Database(object):
  """A single database (set of tables) in the schema."""

  _data_set = False

  def __init__(self, dbh, db_name, char_set=None):
    self._character_set = char_set
    self._dbh = dbh
    self._name = db_name
    # Table names are case-sensitive
    self._tables = {}
    if not self._character_set:
      self.GetCharacterSet()

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
    rows = self._dbh.ExecuteWithRetry(
        'SELECT TABLE_NAME, '
        'SUBSTRING_INDEX(table_collation, \'_\', 1) AS CHARSET '
        'FROM INFORMATION_SCHEMA.TABLES WHERE '
        'TABLE_SCHEMA=%s' % db)
    for row in rows:
      self.AddTable(row['TABLE_NAME'], new_table=False,
                    char_set=row['CHARSET'])

  def AddTable(self, table_name, new_table=True, char_set=None):
    self._LazyInit()
    if table_name in self._tables:
      raise DuplicateNameException('Table `%s` in %s' % (table_name, self))
    char_set = char_set or self.GetCharacterSet()
    self._tables[table_name] = Table(self._dbh, self, table_name,
                                     new_table, char_set)
    return self._tables[table_name]

  def DropTable(self, table_name):
    self.FindTable(table_name)
    del self._tables[table_name]

  def FindTable(self, table_name):
    self._LazyInit()
    if table_name not in self._tables:
      raise UnknownNameException('Table `%s` in %s' % (table_name, self))
    return self._tables[table_name]

  def GetCharacterSet(self):
    """Find the database's default character set."""
    if not self._character_set:
      sql = "SHOW VARIABLES LIKE 'character_set_database'"
      result = self._dbh.ExecuteWithRetry(sql)
      self._character_set = result[0]['Value']
    return self._character_set

  def GetDbName(self):
    if self._name:
      return self._dbh.Escape(self._name)
    return 'DATABASE()'


class Table(object):
  """A single table (list of columns, rows, storage engine) in the schema."""

  _engine = None
  _rows = None
  _data_set = False

  def __init__(self, dbh, database, table_name, new=False, char_set=None):
    self._dbh = dbh
    self._database = database
    self._name = table_name
    # Column names are case-insensitive
    self._columns = {}
    self._primary_key = [] # Ordered list of columns of the PRIMARY KEY.
    if new:
      self._data_set = True
    self._character_set = char_set

  def __str__(self):
    db = str(self._database)
    if db:
      return '%s.`%s`' % (db, self._name)
    else:
      return '`%s`' % self._name

  def _FetchColumns(self):
    """Fetch all columns from information_schema."""
    sql = ('SELECT  COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME '
           'FROM information_schema.COLUMNS '
           'WHERE TABLE_SCHEMA = %(db)s AND TABLE_NAME = %(table)s '
           'ORDER BY ORDINAL_POSITION')
    db_name = self._database.GetDbName()
    table_name = self._dbh.Escape(self._name)
    sql = sql % {'db':db_name, 'table':table_name}
    cols = self._dbh.ExecuteWithRetry(sql)
    for col in cols:
      self.AddColumn(col['COLUMN_NAME'], col['COLUMN_TYPE'],
                     col['CHARACTER_SET_NAME'])

  def _LazyInit(self):
    """Pull the DB information about this object. Delay this if possible."""
    if self._data_set:
      return
    self._data_set = True
    self._FetchColumns()
    self._FetchPrimaryKey()
    db = str(self._database)
    if db:
      prefix = 'SHOW TABLE STATUS FROM %s' % db
    else:
      prefix = 'SHOW TABLE STATUS'
    rows = self._dbh.ExecuteWithRetry(prefix + ' WHERE Name=%(name)s',
                                      { 'name': self._name },
                                      execute=self._dbh.ExecuteMerged)
    engines = set()
    self._rows = 0
    for row in rows:
      engines.add(row['Engine'])
      self._rows = max(self._rows, row['Rows'])
    assert len(engines) == 1, (
        'Engine for table %s differs between shards' % self)
    self._engine = engines.pop()

  def _FetchPrimaryKey(self):
    """Return the list of column names that comprise a table's PRIMARY KEY."""
    sql = ('SELECT column_name '
           'FROM information_schema.statistics '
           'WHERE index_name = "PRIMARY" '
           'AND table_schema = %(db)s AND table_name = %(table)s '
           'ORDER BY SEQ_IN_INDEX')
    db_name = self._database.GetDbName()
    table_name = self._dbh.Escape(self._name)
    sql = sql % {'db':db_name, 'table':table_name}
    res = self._dbh.ExecuteWithRetry(sql)
    self._primary_key = [row['column_name'] for row in res]

  def AddColumn(self, column_name, column_type, char_set=None):
    self._LazyInit()
    column_name = column_name.lower()
    if not char_set:
      char_set = self._database.GetCharacterSet()
    if column_name in self._columns:
      raise DuplicateNameException('Column `%s` in %s' % (column_name, self))
    self._columns[column_name] = Column(self._dbh, self, column_name,
                                        column_type, self.GetCharacterSet())
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

  def GetPrimaryKey(self):
    self._LazyInit()
    return self._primary_key

  def SetEngine(self, engine):
    self._engine = engine

  def GetRows(self):
    self._LazyInit()
    return self._rows

  def GetCharacterSet(self):
    """Get the default character set for a table (Default for the database)."""
    if not self._character_set:
      self._character_set = self._database.GetCharacterSet()
    return self._character_set


class Column(object):
  """A single column (with data type) in the schema."""

  def __init__(self, dbh, table, column_name, column_type, char_set=None):
    self._dbh = dbh
    self._table = table
    self._name = column_name
    self._type = column_type
    if char_set:
      self._character_set = char_set
    else:
      self._character_set = self._table._database.GetCharacterSet()

  def __str__(self):
    return '%s.`%s`' % (self._table, self._name)

  def GetType(self):
    return self._type

  def GetCharacterSet(self):
    return self._character_set
