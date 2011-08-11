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

"""Set of rules to perform on a schema change

The following code visits the syntax tree created by GoogleSQLParser.

The class Visitor traverses the parse tree depth-first. For a given node
visit() will take all the children named node in the tree and try to call
method visit_<node.getName()> if it exists, otherwise the default visit().

Checks can be implemented as a new specialization of Visitor, or
grouped into an existing one. Keep in mind that each new
specialization requires a full tree traversal.
"""

# based on the work of Matthieu Tourne (matthieu.tourne@gmail.com)
__author__ = 'flamingcow@google.com (Ian Gulliver)'

import logging
import pyparsing as pyp
import traceback

import parser
import schema


class Error(Exception): pass


class ValidationError(Error):
  def __init__(self, char, lineno, col, msg):
    self.char = char
    self.lineno = lineno
    self.col = col
    self.msg = msg

  def __str__(self):
    # Match the pyparsing exception string format
    return '%s (at char %d), (line:%d, col:%d)' % (self.msg, self.char,
                                                   self.lineno, self.col)


ParseError = parser.ParseError


def OnlyIfDescendedFrom(tag_names):
  """Decorator that only executes this visitor if we have the named ancestor.

  If one of the specified parent tag names is in the tree above us, run the
  decorated function. Otherwise, continue down the tree by using the default
  visitor, as if the decorated function didn't exist.
  """
  def CalledOncePerDefinition(func):
    def CalledOncePerInvocation(self, *args, **kwargs):
      if self.IsDescendedFrom(tag_names):
        return func(self, *args, **kwargs)
      else:
        return self.visit(*args, **kwargs)
    return CalledOncePerInvocation
  return CalledOncePerDefinition


class Validator(object):
  """Validate a set of parsed SQL statements."""

  def __init__(self, db_schema=None):
    self._schema = db_schema or schema.Schema()
    self._errors = []
    self._warnings = []

  def ValidateTree(self, queries, string=None, additional_visitors=(),
                   max_alter_rows=100000, # for AlterChecker
                   allowed_engines=('InnoDB',), # for CreateTableChecker
                   ):
    """Validate a parse tree.

    Args:
      tokens: pyparsing parse tree
      string: Original string, used for finding problem locations

    Returns:
      Whether the tree validated
    """
    visitors = [
        ShardSetChecker(self._schema, string),
        AlterChecker(self._schema, string, max_alter_rows=max_alter_rows),
        CreateDatabaseChecker(self._schema, string),
        DropDatabaseChecker(self._schema, string),
        CreateTableChecker(self._schema, string,
                           allowed_engines=allowed_engines),
        DropTableChecker(self._schema, string),
        ReplaceChecker(self._schema, string),
        ColumnChecker(self._schema, string),
    ] + list(additional_visitors)

    # We iterate query-by-query, so each visitor can modify the token tree and
    # things happen in the right order.
    for query in queries:
      assert query.getName() == 'query', (
          'Invalid second-level token: %s' % (query.getName()))
      logging.debug('Visiting: %s', query)
      for visitor in visitors:
        visitor.visit([query])

    for visitor in visitors:
      self._errors.extend(visitor.Errors())
      self._warnings.extend(visitor.Warnings())

    return not self._errors and not self._warnings

  def Errors(self):
    return self._errors

  def Warnings(self):
    return self._warnings

  def ValidateString(self, string, parser_class=parser.GoogleSQLParser,
                     **kwargs):
    """Parse a string and validate."""
    schemaparser = parser_class()
    tokens = schemaparser.ParseString(string)
    return self.ValidateTree(tokens, string, **kwargs)


class Visitor(object):
  """Default Visitor.

  Provides default methods to visit nodes and implements some helpful
  helpers.

  Where to keep state:
    Inter-Visitor: on the token objects (AVOID IF POSSIBLE!)
    Inter-statement, intra-Visitor: on the Visitor object
    Intra-statement, intra-Visitor: In visit()'s kwargs
  """

  def __init__(self, db_schema, parsed_string):
    self._errors = []
    self._warnings = []
    self._parsed_str = parsed_string.lower()
    self._db_schema = db_schema
    self._stack = []  # tag names down the tree to our current traversal point

  def _GetDescendants(self, tokens, name, do_not_cross=()):
    """Find all descendants in the tree with a given name.

    Args:
      tokens: Tree root node
      name: Token name to find
      do_not_cross: A list of token names to not recurse past

    Returns:
      A list of tokens of the given name.
    """
    matches = []
    for token in tokens:
      if isinstance(token, pyp.ParseResults):
        logging.debug('GetDescendants: checking %s', token.getName())
        if token.getName() == name:
          matches.append(token)
        if token.getName() in do_not_cross:
          logging.debug('GetDescendants: skipping %s', token.getName())
        else:
          matches.extend(self._GetDescendants(token, name, do_not_cross))
    return matches

  def IsDescendedFrom(self, tag_names):
    """Return true if we are descended from one of tag_names."""
    for tag_name in tag_names:
      if tag_name in self._stack:
        return True
    return False

  def visit(self, tokens, **kwargs):
    """Default visit method.

    For each token, call visit_<node_name> if it exists, otherwise visit.
    """
    for token in tokens:
      if (isinstance(token, pyp.ParseResults) and token.getName()):
        visitor = getattr(self.__class__, 'visit_' + token.getName(),
                          self.__class__.visit)
        self._stack.append(token.getName())
        visitor(self, token, **kwargs)
        assert self._stack.pop() == token.getName(), 'Stack smashed'

  def AddError(self, token, msg):
    """Mark that an error has been detected in the input."""
    if token.loc:
      ex = ValidationError(token.loc,
                           pyp.lineno(token.loc, self._parsed_str),
                           pyp.col(token.loc, self._parsed_str),
                           msg)
    else:
      ex = ValidationError(-1, -1, -1, msg)
    logging.error('%s', ex)
    logging.error('%s', traceback.format_stack()[-2])
    self._errors.append(ex)

  def AddWarning(self, token, msg):
    """Mark that an error has been detected in the input."""
    if token.loc:
      ex = ValidationError(token.loc,
                           pyp.lineno(token.loc, self._parsed_str),
                           pyp.col(token.loc, self._parsed_str),
                           msg)
    else:
      ex = ValidationError(-1, -1, -1, msg)
    logging.warn('%s', ex)
    self._warnings.append(ex)

  def Errors(self):
    return self._errors

  def Warnings(self):
    return self._warnings


class ShardSetChecker(Visitor):
  """Visitor to perform checks on matching shard sets through transactions."""

  _TXN_PARALLEL = [-1]

  def __init__(self, *args, **kwargs):
    Visitor.__init__(self, *args, **kwargs)
    self._in_transaction = False

  def visit_query(self, tokens):
    shard_set = self._GetShardSet(tokens)
    if self._in_transaction and shard_set != self._transaction_shards:
      self.AddError(tokens,
                    'Shard set mismatch within transaction: %s vs. %s' % (
                        shard_set, self._transaction_shards))
    self.visit(tokens, shard_set=shard_set)

  def visit_start_transaction(self, tokens, shard_set):
    if self._in_transaction:
      self.AddError(tokens, 'Start transaction while within a transaction')
    else:
      self._in_transaction = True
    self._transaction_shards = shard_set

  def visit_end_transaction(self, tokens, shard_set):
    if self._in_transaction:
      self._in_transaction = False
    else:
      self.AddError(tokens, 'End transaction while not within a transaction')

  def _GetShardSet(self, tokens):
    running_scheme = self._GetDescendants(tokens, 'running_scheme')
    if running_scheme:
      shards = self._GetDescendants(running_scheme, 'shard')
      if shards:
        return list(shards)
    return self._TXN_PARALLEL


class AlterChecker(Visitor):
  """Visitor to perform checks on ALTER queries."""

  def __init__(self, *args, **kwargs):
    self._max_alter_rows = kwargs.pop('max_alter_rows', 100000)
    Visitor.__init__(self, *args, **kwargs)

  def visit_query(self, tokens):
    self.visit(tokens, running_scheme=tokens.get('running_scheme'))

  def visit_alter(self, tokens, running_scheme):
    if running_scheme and self._GetDescendants(running_scheme, 'shard'):
      self.AddError(tokens, 'ALTER should be run on all shards')

    table_spec = self._GetDescendants(tokens, 'table_spec')
    assert len(table_spec) == 1, 'ALTER requires exactly one table_spec'

    try:
      table = self._db_schema.FindTableFromSpec(table_spec[0])
    except schema.UnknownNameException:
      self.AddError(tokens, 'ALTER on a table that does not exist')
      return
    if table.GetEngine().lower() != 'innodb':
      self.AddError(tokens, 'ALTER on %s, a non-InnoDB table' % table)
    if table.GetRows() > self._max_alter_rows:
      self.AddError(tokens,
                    'ALTER on %s, a %d+ rows table' % (table,
                                                       self._max_alter_rows))

    self.visit(tokens, table=table)

  @OnlyIfDescendedFrom(['alter'])
  def visit_table_flags_definition(self, tokens, table):
    if tokens['table_flags_type'][0] in ('type', 'engine'):
      engine = tokens['table_flags_identifier']
      if engine.lower() != 'innodb':
        self.AddError(tokens, 'ALTER to invalid engine: %s' % engine)

  @OnlyIfDescendedFrom(['alter'])
  def visit_add_column(self, tokens, table):
    """Add a column definition to a table declaration."""
    base_type = 'unknown'
    if 'type' in tokens['column_definition']:
      base_type = tokens['column_definition']['type'][0]
    if 'values' in tokens['column_definition']['type']:
      values = tokens['column_definition']['type']['values']
      base_type = '%s(%s)' % (base_type,
                              ','.join("'%s'" % x.replace("'", "''")
                                       for x in values))
    try:
      column = table.AddColumn(tokens['column_spec']['column'][0], base_type)
      self.visit(tokens, table=table, column=column)
    except schema.DuplicateNameException:
      self.AddError(tokens, 'Column already exists.')

  @OnlyIfDescendedFrom(['alter'])
  def visit_drop_column(self, tokens, table):
    try:
      table.DropColumn(tokens['column_spec']['column'][0])
    except schema.UnknownNameException:
      self.AddError(tokens, 'Unknown column')

  @OnlyIfDescendedFrom(['alter'])
  def visit_modify_column(self, tokens, table):
    try:
      column = table.FindColumn(tokens['column_spec']['column'][0])
      self.visit(tokens, table=table, column=column)
    except schema.UnknownNameException:
      self.AddError(tokens, 'Unknown column')

  @OnlyIfDescendedFrom(['alter'])
  def visit_change_column(self, tokens, table):
    self.visit_drop_column(tokens, table)
    new_tokens = tokens.copy()
    new_tokens['column_spec'] = new_tokens['column_spec_new']
    self.visit_add_column(new_tokens, table)

  @OnlyIfDescendedFrom(['alter'])
  def visit_column_definition(self, tokens, table, column):
    """Compare ENUM modified columns.

    It is only allowed to make changes that add/remove fields from the
    end of ENUM lists.
    """
    if tokens['type']['type_type'] != 'enum':
      return

    # Coerce from ParseResults magic into a real list
    sql_values = tokens['type']['values'][:]  # From local SQL

    # Parse the DBMS derived ENUM using the SQLParser
    db_enum = column.GetType()
    try:
      db_values = parser.SQLParser._ENUM.parseString(db_enum)['values']
    except parser.pyp.ParseException:
      self.AddError(tokens, 'Column %s being converted to an enum' % column)
      return

    common_len = min(len(db_values), len(sql_values))
    if db_values[:common_len] != sql_values[:common_len]:
      self.AddError(tokens,
                    'Base ENUM lists differ: %s vs. %s' % (
                        db_values[:common_len],
                        sql_values[:common_len]))

    default = self._GetDescendants(tokens, 'default')
    if default and 'val' in default[0]:
      default_val = default[0]['val']
      if default_val and default_val[0] not in sql_values:
        self.AddError(tokens,
                      'ENUM default value %r not in values: %r' % (
                          default_val[0], sql_values))

class CreateDatabaseChecker(Visitor):
  """Visitor to perform check on Create Database queries."""

  def visit_query(self, tokens):
    self.visit(tokens, running_scheme=tokens.get('running_scheme'))

  def visit_create_database(self, tokens, running_scheme):
    if running_scheme and self._GetDescendants(running_scheme, 'shard'):
      self.AddError(tokens, 'CREATE should be run on all shards')
    db = self._GetDescendants(tokens, 'database')[0][0]
    try:
      db = self._db_schema.FindDatabase(db)
      self.AddError(tokens, 'Database %s already exists' % db)
    except schema.UnknownNameException:
      self._db_schema.AddDatabase(db)


class DropDatabaseChecker(Visitor):
  """Visitor to perform check on Drop Database queries."""

  def visit_query(self, tokens):
    self.visit(tokens, running_scheme=tokens.get('running_scheme'))

  def visit_drop_database(self, tokens, running_scheme):
    if running_scheme and self._GetDescendants(running_scheme, 'shard'):
      self.AddError(tokens, 'DROP should be run on all shards')
    db = self._GetDescendants(tokens, 'database')[0][0]
    try:
      found_db = self._db_schema.FindDatabase(db)
      self._db_schema.DropDatabase(db)
    except schema.UnknownNameException:
      self.AddError(tokens, 'Database "%s" does not exist, cannot drop.' % db)


class CreateTableChecker(Visitor):
  """Visitor to perform checks on CREATE queries."""

  def __init__(self, *args, **kwargs):
    self._allowed_engines = [engine.lower() for engine
                             in kwargs.pop('allowed_engines', ('InnoDB',))]
    Visitor.__init__(self, *args, **kwargs)

  def visit_query(self, tokens):
    self.visit(tokens, running_scheme=tokens.get('running_scheme'))

  def visit_create_table(self, tokens, running_scheme):
    if running_scheme and self._GetDescendants(running_scheme, 'shard'):
      self.AddError(tokens, 'CREATE should be run on all shards')

    table_spec = self._GetDescendants(tokens, 'table_spec')[0]
    try:
      table = self._db_schema.FindTableFromSpec(table_spec)
      self.AddError(tokens, 'Table %s already exists' % table)
    except schema.UnknownNameException:
      db = self._db_schema.FindDatabaseFromSpec(table_spec)
      table = db.AddTable(table_spec['table'][0])
      self.visit(tokens, table=table)

    engine = False
    for flag in self._GetDescendants(tokens, 'table_flags_definition'):
      if flag['table_flags_type'][0] not in ('type', 'engine'):
        continue
      engine = flag['table_flags_identifier'][0]
    engine = engine or self._db_schema.GetDefaultEngine()
    if engine.lower() not in self._allowed_engines:
      self.AddError(tokens, 'CREATE invalid engine: %s' % engine)
    table.SetEngine(engine)

  @OnlyIfDescendedFrom(['create_table'])
  def visit_operation(self, tokens, table):
    """Add a column definition to a table declaration.

    This trigger is called for each column definition line within a
    CREATE TABLE definition.  The definition is added to the schema
    cache so that future statements that insert data into the table
    can be verified.
    """
    if 'column_spec' in tokens and 'column_definition' in tokens:
      base_type = 'unknown'
      if 'type' in tokens['column_definition']:
        base_type = tokens['column_definition']['type'][0]
      table.AddColumn(tokens['column_spec']['column'][0], base_type)


class DropTableChecker(Visitor):
  """Visitor to perform checks on DROP queries."""

  def visit_query(self, tokens):
    self.visit(tokens, running_scheme=tokens.get('running_scheme'))

  def visit_drop_table(self, tokens, running_scheme):
    if running_scheme and self._GetDescendants(running_scheme, 'shard'):
      self.AddError(tokens, 'DROP should be run on all shards')

    table_spec = self._GetDescendants(tokens, 'table_spec')
    table_name = table_spec[0]['table'][0]
    try:
      self._db_schema.FindDatabaseFromSpec(table_spec[0]).DropTable(table_name)
    except schema.UnknownNameException:
      self.AddError(tokens, 'Table does not exist')


class ReplaceChecker(Visitor):
  """Visitor to perform checks on REPLACE queries."""

  def visit_replace(self, tokens):
    table_spec = self._GetDescendants(tokens, 'table_spec')
    try:
      table = self._db_schema.FindTableFromSpec(table_spec[0])
    except schema.UnknownNameException:
      self.AddError(tokens, 'Unknown table')
      return
    db_columns = set(str(c) for c in table.GetColumns())

    column_specs = self._GetDescendants(tokens, 'column_spec')
    table_name = table_spec[0]['table'][0]
    query_columns = set(spec['column'][0].lower() for spec in column_specs
                        if not spec.get('table')
                        or spec['table'][0] in (None, table_name))
    if query_columns:
      if db_columns != query_columns:
        self.AddError(tokens,
                      'REPLACE doesn\'t specify all columns: %s vs. %s' %
                      (query_columns, db_columns))
    else:
      # No column names, we have to check value counts.
      value_lists = self._GetDescendants(tokens, 'vals')
      for value_list in value_lists:
        if len(value_list) != len(db_columns):
          self.AddError(tokens,
                        'REPLACE value count doesn\'t match DB column count: '
                        '%d vs. %d' % (len(value_list), len(db_columns)))


class ColumnChecker(Visitor):
  """Validate that columns used in DML contexts exist."""

  def _GetTableAliases(self, tokens):
    table_aliases = {}
    for alias in self._GetDescendants(tokens, 'table_alias', ['source_select']):
      alias_name = alias.get('alias')
      if alias_name:
        table_aliases[alias_name[0]] = alias['table_spec']
    for table in self._GetDescendants(tokens, 'table_spec', ['source_select']):
      table_aliases[table['table'][0]] = table
    return table_aliases

  def visit_statement(self, tokens):
    self.visit(tokens, table_aliases=self._GetTableAliases(tokens))

  def visit_source_select(self, tokens, table_aliases):
    # Ignore parent's table_aliases, as we're now in our own namespace
    self.visit(tokens, table_aliases=self._GetTableAliases(tokens))

  @OnlyIfDescendedFrom(['insert', 'update', 'delete', 'replace', 'select'])
  def visit_table_spec(self, tokens, table_aliases):
    try:
      self._db_schema.FindTableFromSpec(tokens, table_aliases)
    except schema.UnknownNameException:
      self.AddError(tokens, 'Unknown table')

  @OnlyIfDescendedFrom(['insert', 'update', 'delete', 'replace', 'select'])
  def visit_column_spec(self, tokens, table_aliases):
    # Is the spec enough to find a column?
    try:
      self._db_schema.FindColumnFromSpec(tokens, table_aliases)
    except KeyError:
      # Not enough info, we have to scan
      column_name = tokens['column'][0]
      results = set()
      logging.debug('Searching for %s in: %s', column_name, table_aliases)
      for table_spec in table_aliases.itervalues():
        try:
          table = self._db_schema.FindTableFromSpec(table_spec)
        except schema.UnknownNameException:
          self.AddError(tokens, 'Unknown table')
          continue
        try:
          results.add(table.FindColumn(column_name))
        except schema.UnknownNameException:
          pass
      if not results:
        self.AddError(tokens, 'Unknown column')
      elif self.IsDescendedFrom(['using']):
        if len(results) != 2:
          self.AddError(tokens,
                        'Columns in USING must be in exactly two tables: %s' %
                        [str(r) for r in results])
      elif len(results) != 1:
        self.AddError(tokens,
                      'Ambiguous column: %s' % [str(r) for r in results])
    except schema.UnknownNameException:
      # Enough info, but doesn't exist
      self.AddError(tokens, 'Unknown column')

class TouchChecker(Visitor):
  """Warn when a particular table/column is touched."""

  def __init__(self, *args, **kwargs):
    self._tables = set(kwargs.pop('tables', ()))
    self._columns = set(x.lower() for x in kwargs.pop('columns', ()))
    self._values = set(kwargs.pop('values', ()))
    self._operations = set(kwargs.pop('operations', ()))
    self._msg = kwargs.pop('message')
    Visitor.__init__(self, *args, **kwargs)

  def visit_statement(self, tokens):
    tables = set()
    if self._tables:
      tables = set(str(x[0]) for x in self._GetDescendants(tokens, 'table'))
      if not self._tables & tables:
        # No tables in common
        return

    columns = set()
    if self._columns:
      columns = set(str(x[0]).lower()
                    for x in self._GetDescendants(tokens, 'column'))
      if not self._columns & columns:
        # No columns in common
        return

    values = set()
    if self._values:
      values = set(str(x[0]) for x in self._GetDescendants(tokens, 'val'))
      if not self._values & values:
        # No values in common
        return

    operations = set()
    if self._operations:
      for operation in self._operations:
        if self._GetDescendants(tokens, operation):
          operations.add(operation)
      if not operations:
        # No such operations found
        return

    msg = self._msg % {
        'tables': tables,
        'columns': columns,
        'values': values,
        'operations': operations,
    }
    self.AddWarning(tokens, msg)
