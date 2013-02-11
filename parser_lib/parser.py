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

"""MySQL query parser.

Tools to parse SQL queries into a pyparsing parse tree. The primary method here
is SQLParser.ParseString, which takes a string that you might pipe to the mysql
CLI (containing multiple delimited queries) and parses it. The parsing grammar
is far from complete, and focuses on DDL.

GoogleSQLParser adds ON SHARD support to the grammar.
"""

# based on the work of Matthieu Tourne (matthieu.tourne@gmail.com)
__author__ = 'flamingcow@google.com (Ian Gulliver)'

import logging
import pyparsing as pyp
import re

try:
  from ..pylib import db
except (ImportError, ValueError):
  from pylib import db


class Error(Exception): pass


class ParseError(Error):
  def __init__(self, msg, loc):
    self.msg = msg
    self.loc = loc

  def __str__(self):
    return '%s (at char %d)' % (self.msg, self.loc)


class SQLParser(object):
  """SQL Parser"""

  def _LogStart(self, instring, loc, expr):
    logging.debug('Start: base_loc: %d, loc: %d, expr: %s',
                  self._base_loc, loc, expr.name)

  def _LogSuccess(self, instring, start, loc, expr, tokens):
    logging.debug('Success: base_loc: %d, loc: %d, expr: %s, tokens: %s',
                  self._base_loc, loc, expr.name, tokens)
    tokens['loc'] = self._base_loc + loc

  def _LogFailure(self, instring, start, expr, err):
    logging.debug('Failure: base_loc: %d, loc: %d, expr: %s, err: %s',
                  self._base_loc, err.loc, expr.name, err)

  def __init__(self, progress_callback=None):
    """Constructor.

    Args:
      progress_callback: If specified, called with the character location of
        the end of the last-yielded statement.
    """
    # Get all the class variables that matches _*_TOKEN
    keywords = list(SQLParser.__dict__[k]
                    for k in SQLParser.__dict__
                    if re.match(r'^_([_\w])+_TOKEN$', k))
    # Fill the grammar rule _KEYWORDS with all the keywords possible
    SQLParser.__dict__['_KEYWORDS'] << pyp.MatchFirst(keywords)

    self._loc = 0  # Last yielded line end
    self._base_loc = 0  # Start of this statement
    self._callback = progress_callback

    for key in dir(self):
      grammar_rule = getattr(self, key)
      if isinstance(grammar_rule, pyp.ParserElement):
        grammar_rule.setName(key)
        grammar_rule.setDebugActions(
            self._LogStart, self._LogSuccess, self._LogFailure)

  def _OnNewLine(self, loc):
    self._loc = loc

  def ParseString(self, string):
    logging.debug('Parsing: %r', string)
    try:
      for statement in db.XCombineSQL(db.XSplit(string, '\n',
                                                callback=self._OnNewLine)):
        yield self._QUERY.parseString(statement)[0]
        if self._callback:
          self._callback(self._loc)
        self._base_loc = self._loc + len(statement) + 1
    except pyp.ParseException as e:
      raise ParseError(e.msg, self._base_loc + e.loc)
    except db.InputRemaining as e:
      raise ParseError('Input remaining: %s' % e, self._base_loc + self._loc)

  # DISCARDED

  _COMMENT_START = pyp.Keyword(
      '--', identChars=pyp.Keyword.DEFAULT_KEYWORD_CHARS + '-')
  _COMMENT_LINE = _COMMENT_START + pyp.restOfLine
  _COMMENT_BLOCK = pyp.Regex(r'/\*(?=[^!])(?:[^*]*\*+)+?/')

  # TERMINALS

  _LINE_DELIMITER = pyp.Suppress(';').setName(';')

  _ALTER_TOKEN = pyp.CaselessKeyword('alter')
  _SELECT_TOKEN = pyp.CaselessKeyword('select')
  _CREATE_TOKEN = pyp.CaselessKeyword('create')
  _UPDATE_TOKEN = pyp.CaselessKeyword('update')
  _INSERT_TOKEN = pyp.CaselessKeyword('insert')
  _REPLACE_TOKEN = pyp.CaselessKeyword('replace')
  _DELETE_TOKEN = pyp.CaselessKeyword('delete')

  _MODIFY_TOKEN = pyp.CaselessKeyword('modify')
  _ADD_TOKEN = pyp.CaselessKeyword('add')
  _CHANGE_TOKEN = pyp.CaselessKeyword('change')
  _DROP_TOKEN = pyp.CaselessKeyword('drop')
  _CONVERT_TOKEN = pyp.CaselessKeyword('convert')
  _TO_TOKEN = pyp.CaselessKeyword('to')

  _ALL_TOKEN = pyp.CaselessKeyword('all')
  _DISTINCT_TOKEN = pyp.CaselessKeyword('distinct')
  _DISTINCTROW_TOKEN = pyp.CaselessKeyword('distinctrow')

  _FROM_TOKEN = pyp.CaselessKeyword('from').suppress()
  _WHERE_TOKEN = pyp.CaselessKeyword('where').suppress()
  _ORDER_TOKEN = pyp.CaselessKeyword('order').suppress()
  _GROUP_TOKEN = pyp.CaselessKeyword('group').suppress()
  _HAVING_TOKEN = pyp.CaselessKeyword('having').suppress()
  _LIMIT_TOKEN = pyp.CaselessKeyword('limit').suppress()
  _BY_TOKEN = pyp.CaselessKeyword('by').suppress()
  _AS_TOKEN = pyp.CaselessKeyword('as').suppress()

  _INTO_TOKEN = pyp.CaselessKeyword('into').suppress()
  _VALUES_TOKEN = pyp.CaselessKeyword('values').suppress()

  _IS_TOKEN = pyp.CaselessKeyword('is')
  _NOT_TOKEN = pyp.CaselessKeyword('not')
  _NULL_TOKEN = pyp.CaselessKeyword('null')
  _TRUE_TOKEN = pyp.CaselessKeyword('true')
  _FALSE_TOKEN = pyp.CaselessKeyword('false')
  _UNKNOWN_TOKEN = pyp.CaselessKeyword('unknown')
  _IN_TOKEN = pyp.CaselessKeyword('in')
  _CASE_TOKEN = pyp.CaselessKeyword('case')
  _WHEN_TOKEN = pyp.CaselessKeyword('when')
  _THEN_TOKEN = pyp.CaselessKeyword('then')
  _ELSE_TOKEN = pyp.CaselessKeyword('else')
  _START_TOKEN = pyp.CaselessKeyword('start')
  _END_TOKEN = pyp.CaselessKeyword('end')

  _JOIN_TOKEN = pyp.CaselessKeyword('join')
  _LEFT_TOKEN = pyp.CaselessKeyword('left')
  _RIGHT_TOKEN = pyp.CaselessKeyword('right')
  _CROSS_TOKEN = pyp.CaselessKeyword('cross')
  _INNER_TOKEN = pyp.CaselessKeyword('inner')
  _OUTER_TOKEN = pyp.CaselessKeyword('outer')
  _NATURAL_TOKEN = pyp.CaselessKeyword('natural')
  _ON_TOKEN = pyp.CaselessKeyword('on')
  _USING_TOKEN = pyp.CaselessKeyword('using')
  _STRAIGHT_JOIN_TOKEN = pyp.CaselessKeyword('straight_join')

  _LIKE_TOKEN = pyp.CaselessKeyword('like')
  _ENGINE_TOKEN = pyp.CaselessKeyword('engine')
  _IF_TOKEN = pyp.CaselessKeyword('if').suppress()
  _EXISTS_TOKEN = pyp.CaselessKeyword('exists').suppress()
  _CHARSET_TOKEN = pyp.CaselessKeyword('charset')
  _CHARACTER_TOKEN = pyp.CaselessKeyword('character')
  _NAMES_TOKEN = pyp.CaselessKeyword('names')
  _COLLATE_TOKEN = pyp.CaselessKeyword('collate')
  _INTERVAL_TOKEN = pyp.CaselessKeyword('interval')

  _DATABASE_TOKEN = pyp.CaselessKeyword('database')
  _TABLE_TOKEN = pyp.CaselessKeyword('table').suppress()
  _COLUMN_TOKEN = pyp.CaselessKeyword('column').suppress()
  _INDEX_TOKEN = pyp.CaselessKeyword('index')
  _PRIMARY_TOKEN = pyp.CaselessKeyword('primary')
  _KEY_TOKEN = pyp.CaselessKeyword('key')
  _UNIQUE_TOKEN = pyp.CaselessKeyword('unique')
  _DUPLICATE_TOKEN = pyp.CaselessKeyword('duplicate').suppress()
  _AUTO_INCREMENT_TOKEN = pyp.CaselessKeyword('auto_increment')
  _DEFAULT_TOKEN = pyp.CaselessKeyword('default').suppress()
  _USE_TOKEN = pyp.CaselessKeyword('use')
  _IGNORE_TOKEN = pyp.CaselessKeyword('ignore')
  _FORCE_TOKEN = pyp.CaselessKeyword('force')
  _CONSTRAINT_TOKEN = pyp.CaselessKeyword('constraint')
  _FOREIGN_TOKEN = pyp.CaselessKeyword('foreign')
  _RESTRICT_TOKEN = pyp.CaselessKeyword('restrict')
  _CASCADE_TOKEN = pyp.CaselessKeyword('cascade')
  _NO_TOKEN = pyp.CaselessKeyword('no')
  _ACTION_TOKEN = pyp.CaselessKeyword('action')
  _REFERENCES_TOKEN = pyp.CaselessKeyword('references')

  _TINYINT_TOKEN = pyp.CaselessKeyword('tinyint')
  _SMALLINT_TOKEN = pyp.CaselessKeyword('smallint')
  _MEDIUMINT_TOKEN = pyp.CaselessKeyword('mediumint')
  _INT_TOKEN = pyp.CaselessKeyword('int')
  _INTEGER_TOKEN = pyp.CaselessKeyword('integer')
  _BIGINT_TOKEN = pyp.CaselessKeyword('bigint')

  _UNSIGNED_TOKEN = pyp.CaselessKeyword('unsigned')

  _DECIMAL_TOKEN = pyp.CaselessKeyword('decimal')
  _DEC_TOKEN = pyp.CaselessKeyword('dec')
  _FIXED_TOKEN = pyp.CaselessKeyword('fixed')
  _FLOAT_TOKEN = pyp.CaselessKeyword('float')
  _DOUBLE_TOKEN = pyp.CaselessKeyword('double')
  _PRECISION_TOKEN = pyp.CaselessKeyword('precision')

  _DATE_TOKEN = pyp.CaselessKeyword('date')
  _DATETIME_TOKEN = pyp.CaselessKeyword('datetime')
  _TIMESTAMP_TOKEN = pyp.CaselessKeyword('timestamp')
  _TIME_TOKEN = pyp.CaselessKeyword('time')
  _YEAR_TOKEN = pyp.CaselessKeyword('year')

  _CHAR_TOKEN = pyp.CaselessKeyword('char')
  _VARCHAR_TOKEN = pyp.CaselessKeyword('varchar')
  _BINARY_TOKEN = pyp.CaselessKeyword('binary')
  _VARBINARY_TOKEN = pyp.CaselessKeyword('varbinary')

  _TINYBLOB_TOKEN = pyp.CaselessKeyword('tinyblob')
  _BLOB_TOKEN = pyp.CaselessKeyword('blob')
  _MEDIUMBLOB_TOKEN = pyp.CaselessKeyword('mediumblob')
  _LONGBLOB_TOKEN = pyp.CaselessKeyword('longblob')
  _TINYTEXT_TOKEN = pyp.CaselessKeyword('tinytext')
  _TEXT_TOKEN = pyp.CaselessKeyword('text')
  _MEDIUMTEXT_TOKEN = pyp.CaselessKeyword('mediumtext')
  _LONGTEXT_TOKEN = pyp.CaselessKeyword('longtext')

  _ENUM_TOKEN = pyp.CaselessKeyword('enum')
  _SET_TOKEN = pyp.CaselessKeyword('set')

  _BIT_TOKEN = pyp.CaselessKeyword('bit')

  _FIRST_TOKEN = pyp.CaselessKeyword('first')
  _BEFORE_TOKEN = pyp.CaselessKeyword('before')
  _AFTER_TOKEN = pyp.CaselessKeyword('after')

  _CURRENT_TIMESTAMP_TOKEN = pyp.CaselessKeyword('current_timestamp')

  _BEGIN_TOKEN = pyp.CaselessKeyword('begin')
  _TRANSACTION_TOKEN = pyp.CaselessKeyword('transaction')
  _COMMIT_TOKEN = pyp.CaselessKeyword('commit')
  _ROLLBACK_TOKEN = pyp.CaselessKeyword('rollback')

  _LOCAL_TOKEN = pyp.CaselessKeyword('local')
  _SESSION_TOKEN = pyp.CaselessKeyword('session')
  _GLOBAL_TOKEN = pyp.CaselessKeyword('global')

  ## IDENTIFIER

  _KEYWORDS = pyp.Forward()  # list of keywords, defined by __init__()

  _IDENTIFIER = pyp.Group(pyp.Word(pyp.alphas, pyp.alphanums + '_$')
                          | pyp.QuotedString('`', multiline=True, escChar='\\'))

  _CHARSET = '_' + pyp.Word(pyp.alphanums).setResultsName('character_set')

  _STRING = (pyp.Optional(_CHARSET)
             + (pyp.QuotedString('\'', multiline=True, escChar='\\')
                | pyp.QuotedString('\"', multiline=True, escChar='\\')))

  _NUMBER = pyp.Word(pyp.nums)

  _ARITH_SIGN = pyp.Word('+-', exact=1)
  _E = pyp.CaselessLiteral('E')

  _REAL_NUMBER = pyp.Combine(pyp.Optional(_ARITH_SIGN)
                             + pyp.Optional(_NUMBER) + '.' + _NUMBER
                             + pyp.Optional(_E
                                            + pyp.Optional(_ARITH_SIGN)
                                            + _NUMBER))
  _INT_NUMBER = pyp.Combine(pyp.Optional(_ARITH_SIGN)
                            + _NUMBER
                            + pyp.Optional(_E
                                           + pyp.Optional('+')
                                           + _NUMBER))

  _HEX = ((pyp.CaselessLiteral('0x').suppress()
           + pyp.Word(pyp.hexnums))
          | pyp.Regex(r"x'(?:[0-9a-fA-F])+'"))

  _VAL = pyp.Group(
          _HEX
          | pyp.OneOrMore(_STRING)
          | _REAL_NUMBER
          | _INT_NUMBER
          | _NULL_TOKEN
          | _TRUE_TOKEN
          | _FALSE_TOKEN).setResultsName('val')

  ## TYPES

  _FIELD_LIST = pyp.Group(pyp.Suppress('(')
                          + pyp.delimitedList(_IDENTIFIER)
                          + pyp.Suppress(')')
                          ).setResultsName('fields')

  _STRING_LIST = pyp.Group(pyp.Suppress('(')
                           + pyp.delimitedList(_STRING)
                           + pyp.Suppress(')')
                           ).setResultsName('values')

  _TYPE_SIZE = (pyp.Suppress('(')
                + _NUMBER.setName('type_size')
                + pyp.Suppress(')'))

  _TYPE_PRECISION = (pyp.Suppress('(')
                     + _NUMBER.setName('type_precision')
                     + pyp.Suppress(',')
                     + _NUMBER.setName('type_scale')
                     + pyp.Suppress(')'))

  # Types that don't take arguments.
  _SIMPLE_TYPE = (_DATE_TOKEN
                  | _DATETIME_TOKEN
                  | _TIMESTAMP_TOKEN
                  | _TIME_TOKEN
                  | _YEAR_TOKEN
                  | _TINYTEXT_TOKEN
                  | _TEXT_TOKEN
                  | _MEDIUMTEXT_TOKEN
                  | _LONGTEXT_TOKEN
                  | _TINYBLOB_TOKEN
                  | _BLOB_TOKEN
                  | _MEDIUMBLOB_TOKEN
                  | _LONGBLOB_TOKEN).setResultsName('type_type')

  _BIT = (_BIT_TOKEN.setResultsName('type_type')
          + pyp.Optional(_TYPE_SIZE))

  _ENUM = (_ENUM_TOKEN.setResultsName('type_type')
           + _STRING_LIST)

  _SET_TYPE = (_SET_TOKEN.setResultsName('type_type')
               + _STRING_LIST)

  _INTS = ((_TINYINT_TOKEN
            | _SMALLINT_TOKEN
            | _MEDIUMINT_TOKEN
            | _INT_TOKEN
            | _INTEGER_TOKEN
            | _BIGINT_TOKEN).setResultsName('type_type')
           + pyp.Optional(_TYPE_SIZE)
           + pyp.Optional(_UNSIGNED_TOKEN))

  _REALS = ((_DECIMAL_TOKEN
             | _DEC_TOKEN
             | _FIXED_TOKEN
             | _FLOAT_TOKEN
             | _DOUBLE_TOKEN + pyp.Optional(_PRECISION_TOKEN)
             ).setResultsName('type_type')
            + pyp.Optional(_TYPE_PRECISION))

  _CHARS = ((_VARCHAR_TOKEN
             | _CHAR_TOKEN
             | _BINARY_TOKEN
             | _VARBINARY_TOKEN).setResultsName('type_type')
            + pyp.Optional(_TYPE_SIZE)
            + pyp.Optional(_BINARY_TOKEN))

  _TYPE = pyp.Group(_BIT
                    | _ENUM
                    | _SET_TYPE
                    | _INTS
                    | _REALS
                    | _CHARS
                    | _SIMPLE_TYPE
                    ).setResultsName('type')

  ## GRAMMAR

  # COMMONS

  _DB_NAME = _IDENTIFIER.setResultsName('database')

  _TABLE_NAME_ONLY = _IDENTIFIER.setResultsName('table')

  _TABLE_NAME = pyp.Group((_DB_NAME + '.' + _TABLE_NAME_ONLY)
                          | _TABLE_NAME_ONLY).setResultsName('table_spec')

  _COLUMN_NAME_WILD = (_IDENTIFIER | '*').setResultsName('column')

  _COLUMN_NAME = pyp.Group(
      (_DB_NAME + '.' + _TABLE_NAME_ONLY + '.' + _COLUMN_NAME_WILD)
      | (_TABLE_NAME_ONLY + '.' + _COLUMN_NAME_WILD)
      | _COLUMN_NAME_WILD).setResultsName('column_spec')

  _INDEX_NAME = _IDENTIFIER.setResultsName('index')


  _COLUMN_LIST = pyp.Group(pyp.Suppress('(')
                           + pyp.delimitedList(_COLUMN_NAME)
                           + pyp.Suppress(')')
                           ).setResultsName('columns')

  # DATA DEFINITION COMMONS

  _DEFAULT_VAL = (_DEFAULT_TOKEN
                  + pyp.Group(_NULL_TOKEN
                              | _VAL
                              | _CURRENT_TIMESTAMP_TOKEN
                              ).setResultsName('default'))

  _COLUMN_CONSTRAINT = pyp.Group(pyp.Optional(_NOT_TOKEN)
                                 + _NULL_TOKEN
                                 ).setResultsName('constraint')

  _POSITIONAL = pyp.Group(_FIRST_TOKEN
                          | ((_BEFORE_TOKEN | _AFTER_TOKEN) + _COLUMN_NAME)
                          ).setResultsName('position')

  # Optional column flags:
  #  - CHARSET <charset>
  #  - CHARACTER SET <charset>
  #  - COLLATE <collate name>
  #  - DEFAULT '<value>'
  #  - AUTO_INCREMENT
  #  - NOT NULL
  #  - ON UPDATE CURRENT_TIMESTAMP
  _COLUMN_FLAGS = pyp.Group(
      (_CHARSET_TOKEN + _IDENTIFIER.setResultsName('charset'))
      | (_CHARACTER_TOKEN + _SET_TOKEN + _IDENTIFIER.setResultsName('charset'))
      | (_COLLATE_TOKEN + _IDENTIFIER.setResultsName('collate'))
      | _COLUMN_CONSTRAINT
      | _DEFAULT_VAL
      | _AUTO_INCREMENT_TOKEN.setResultsName('option')
      | (_ON_TOKEN + _UPDATE_TOKEN + _CURRENT_TIMESTAMP_TOKEN)
      ).setResultsName('column_flags')

  _COLUMN_DEFINITION = pyp.Group(_TYPE
                                 + pyp.ZeroOrMore(_COLUMN_FLAGS)
                                 ).setResultsName('column_definition')

  _KEY_DEFINITION = pyp.Group(
      (((pyp.Optional(_UNIQUE_TOKEN).setResultsName('key_option')
         + (_INDEX_TOKEN | _KEY_TOKEN).setResultsName('key_type'))
        | _UNIQUE_TOKEN.setResultsName('key_type'))
       + pyp.Optional(_IDENTIFIER).setResultsName('key_name')
       + _FIELD_LIST)
      | ((_PRIMARY_TOKEN + _KEY_TOKEN).setResultsName('key_type')
         + _FIELD_LIST)
      ).setResultsName('key_definition')


  # ALTER STATEMENTS

  # ADD COLUMN columnname TYPE [BEFORE | AFTER ...]
  # ADD COLUMN (columnname TYPE, ...) [BEFORE | AFTER ...]
  _ALTER_TABLE_ADD_COLUMN = pyp.Group(
      _ADD_TOKEN + pyp.Optional(_COLUMN_TOKEN)
      + ((_COLUMN_NAME + _COLUMN_DEFINITION)
         | (pyp.Suppress('(')
            + pyp.delimitedList(_COLUMN_NAME + _COLUMN_DEFINITION)
            + pyp.Suppress(')')))
      + pyp.ZeroOrMore(_COLUMN_FLAGS)
      + pyp.Optional(_PRIMARY_TOKEN + _KEY_TOKEN)
      + pyp.Optional(_POSITIONAL)
      ).setResultsName('add_column')

  _REFERENCE_OPTION = pyp.Group(
      _RESTRICT_TOKEN
      | _CASCADE_TOKEN
      | (_SET_TOKEN + _NULL_TOKEN)
      | (_NO_TOKEN + _ACTION_TOKEN)
  ).setResultsName('reference_option')

  _CONSTRAINT_DEFINITION = pyp.Group(
    pyp.Optional(
        _CONSTRAINT_TOKEN
        + pyp.Optional(_IDENTIFIER).setResultsName('constraint_name')
        )
    + _FOREIGN_TOKEN + _KEY_TOKEN
    + pyp.Optional(_IDENTIFIER).setResultsName('key_name')
    + _FIELD_LIST
    + _REFERENCES_TOKEN
    + _TABLE_NAME
    + _FIELD_LIST
    + pyp.Optional(_ON_TOKEN
                   + _DELETE_TOKEN
                   + _REFERENCE_OPTION)
    + pyp.Optional(_ON_TOKEN
                   + _UPDATE_TOKEN
                   + _REFERENCE_OPTION)
  )

  _ALTER_TABLE_ADD_CONSTRAINT = pyp.Group(
      _ADD_TOKEN
      + _CONSTRAINT_DEFINITION
      ).setResultsName('add_constraint')

  _ALTER_TABLE_DROP_FOREIGN_KEY = pyp.Group(
      _DROP_TOKEN
      + _FOREIGN_TOKEN
      + _KEY_TOKEN
      + _IDENTIFIER.setResultsName('constraint_name')
      ).setResultsName('drop_foreign_key')

  # ADD [UNIQUE] INDEX | KEY ...
  # ADD UNIQUE ...
  _ALTER_TABLE_ADD_INDEX = pyp.Group(
      _ADD_TOKEN
      + ((pyp.Optional(_UNIQUE_TOKEN).setResultsName('key_option')
          + (_INDEX_TOKEN | _KEY_TOKEN))
         | (_UNIQUE_TOKEN).setResultsName('key_type'))
      + pyp.Optional(_IDENTIFIER).setResultsName('key_name')
      + _FIELD_LIST
      ).setResultsName('add_index')

  _ALTER_TABLE_ADD_PRIMARY_KEY = pyp.Group(
      _ADD_TOKEN + _PRIMARY_TOKEN + _KEY_TOKEN
      + _FIELD_LIST
      ).setResultsName('add_primary_key')

  _ALTER_TABLE_ALTER = pyp.Group(
      _ALTER_TOKEN + pyp.Optional(_COLUMN_TOKEN)
      + _COLUMN_NAME
      + ((_SET_TOKEN + _DEFAULT_VAL)
         | (_DROP_TOKEN + _DEFAULT_TOKEN))
      ).setResultsName('alter_column')

  _ALTER_TABLE_MODIFY = pyp.Group(
      _MODIFY_TOKEN + pyp.Optional(_COLUMN_TOKEN)
      + (_COLUMN_NAME + _COLUMN_DEFINITION)
      + pyp.Optional(_POSITIONAL)
      ).setResultsName('modify_column')

  _ALTER_TABLE_CHANGE = pyp.Group(
      _CHANGE_TOKEN + pyp.Optional(_COLUMN_TOKEN)
      + _COLUMN_NAME
      + _COLUMN_NAME.setResultsName('column_spec_new')
      + _COLUMN_DEFINITION
      ).setResultsName('change_column')

  _ALTER_TABLE_DROP_COLUMN = pyp.Group(
      _DROP_TOKEN + pyp.Optional(_COLUMN_TOKEN)
      + _COLUMN_NAME
      ).setResultsName('drop_column')

  _ALTER_TABLE_DROP_PRIMARY_KEY = pyp.Group(
      _DROP_TOKEN + _PRIMARY_TOKEN + _KEY_TOKEN
      ).setResultsName('drop_primary_key')

  _ALTER_TABLE_DROP_INDEX = pyp.Group(
      _DROP_TOKEN + (_INDEX_TOKEN | _KEY_TOKEN)
      + _IDENTIFIER.setResultsName('key_name')
      ).setResultsName('drop_index')

  _ALTER_TABLE_CONVERT = pyp.Group(
      _CONVERT_TOKEN + _TO_TOKEN + _CHARACTER_TOKEN + _SET_TOKEN
      + _IDENTIFIER.setResultsName('character_set')
      ).setResultsName('convert')

  _ALTER_CHARACTER_SET = pyp.Group(
      _CHARACTER_TOKEN + _SET_TOKEN
      + _IDENTIFIER.setResultsName('character_set')
      ).setResultsName('alter_charset')

  # The various ALTER TABLE operations supported:
  # - ADD PRIMARY KEY
  # - ADD INDEX
  # - ADD COLUMN
  # - CHANGE
  # - DROP
  # - ALTER
  _ALTER_TABLE_OPERATIONS = pyp.Group(
      _ALTER_TABLE_MODIFY
      | _ALTER_TABLE_ADD_PRIMARY_KEY
      | _ALTER_TABLE_ADD_CONSTRAINT
      | _ALTER_TABLE_DROP_FOREIGN_KEY
      | _ALTER_TABLE_ADD_INDEX
      | _ALTER_TABLE_ADD_COLUMN
      | _ALTER_TABLE_CHANGE
      | _ALTER_TABLE_DROP_PRIMARY_KEY
      | _ALTER_TABLE_DROP_INDEX
      | _ALTER_TABLE_DROP_COLUMN
      | _ALTER_TABLE_ALTER
      | _ALTER_TABLE_CONVERT
      | _ALTER_CHARACTER_SET
      ).setResultsName('operations')

  _ALTER_TABLE_SQL = pyp.Group(_ALTER_TOKEN
                               + _TABLE_TOKEN
                               + _TABLE_NAME
                               + pyp.delimitedList(_ALTER_TABLE_OPERATIONS)
                               ).setResultsName('alter')

  _ALTER_DATABASE_OPERATIONS = pyp.Group(
      _ALTER_CHARACTER_SET
      ).setResultsName('operations')

  _ALTER_DATABASE_SQL = pyp.Group(
      _ALTER_TOKEN
      + _DATABASE_TOKEN
      + _DB_NAME
      + pyp.delimitedList(_ALTER_DATABASE_OPERATIONS)
      ).setResultsName('alter_db')

  # CREATE STATEMENTS

  _CREATE_DEFINITION = pyp.Group(_KEY_DEFINITION
                                 | _CONSTRAINT_DEFINITION
                                 | (_COLUMN_NAME
                                    + _COLUMN_DEFINITION)
                                 ).setResultsName('operation')

  # Match on IF NOT EXISTS
  _CREATE_NO_OVERWRITE = _IF_TOKEN + _NOT_TOKEN + _EXISTS_TOKEN

  _CREATE_OPERATIONS = pyp.Group(pyp.delimitedList(_CREATE_DEFINITION)
                                 ).setResultsName('operations')

  # CREATE TABLE table options can come in any order.  There may be
  # zero or many of them
  _TABLE_FLAGS = pyp.Group(_ENGINE_TOKEN
                           | (_DEFAULT_TOKEN + _CHARSET_TOKEN)
                           | _CHARSET_TOKEN
                           | (_CHARACTER_TOKEN + _SET_TOKEN)
                           | (_DEFAULT_TOKEN + _CHARACTER_TOKEN + _SET_TOKEN)
                           | _COLLATE_TOKEN
                           ).setResultsName('table_flags_type')

  # CREATE TABLE table options are always of the format: FLAG=VALUE
  _TABLE_FLAGS_DEF = pyp.Group(
      _TABLE_FLAGS
      + pyp.Optional(pyp.Suppress('='))
      + _IDENTIFIER.setResultsName('table_flags_identifier')
      ).setResultsName('table_flags_definition')

  _CREATE_TABLE_SQL = pyp.Group(
      _CREATE_TOKEN
      + _TABLE_TOKEN
      + pyp.Optional(_CREATE_NO_OVERWRITE)
      + _TABLE_NAME
      + pyp.Suppress('(')
      + _CREATE_OPERATIONS
      + pyp.Suppress(')')
      + pyp.ZeroOrMore(_TABLE_FLAGS_DEF).setResultsName('table_flags')
      ).setResultsName('create_table')

  _CREATE_TABLE_LIKE_SQL = pyp.Group(
      _CREATE_TOKEN
      + _TABLE_TOKEN
      + pyp.Optional(_CREATE_NO_OVERWRITE)
      + _TABLE_NAME
      + _LIKE_TOKEN
      + _TABLE_NAME
      ).setResultsName('create_table_like')

  # DROP TABLE [IF EXISTS] table
  _DROP_TABLE_SQL = pyp.Group(_DROP_TOKEN
                              + _TABLE_TOKEN
                              + pyp.Optional(_IF_TOKEN + _EXISTS_TOKEN)
                              + pyp.delimitedList(_TABLE_NAME)
                              ).setResultsName('drop_table')

  # CREATE DATABASE dbname
  _CREATE_DATABASE_SQL = pyp.Group(_CREATE_TOKEN
                                   + _DATABASE_TOKEN
                                   + pyp.Optional(_CREATE_NO_OVERWRITE)
                                   + _DB_NAME
                                   ).setResultsName('create_database')

  # DROP DATABASE dbname
  _DROP_DATABASE_SQL = pyp.Group(_DROP_TOKEN
                                 + _DATABASE_TOKEN
                                 + pyp.Optional(_IF_TOKEN + _EXISTS_TOKEN)
                                 + _DB_NAME
                                 ).setResultsName('drop_database')

  # CREATE INDEX idx ON table (column, ...)
  _CREATE_INDEX_SQL = (
      _CREATE_TOKEN
      + pyp.Optional(_UNIQUE_TOKEN).setResultsName('key_option')
      + _INDEX_TOKEN
      + _INDEX_NAME.setResultsName('key_name')
      + _ON_TOKEN
      + _TABLE_NAME
      + _COLUMN_LIST)

  # EXPRESSIONS

  _BINOP1 = pyp.oneOf("* / %")
  _BINOP2 = pyp.oneOf("+ - << >> | &")
  _BINOP3 = pyp.oneOf(":= = != <> < > >= <=")
  _BINOP4 = pyp.oneOf("like between regexp", caseless=True)  # optional "NOT"
  _BINOP5 = pyp.oneOf("and", caseless=True)
  _BINOP6 = pyp.oneOf("or", caseless=True)

  _EXPRESSION = pyp.Forward()  # _EXPRESSION is recursive

  _DATE_FUNCTION_NAME = pyp.oneOf("date_add date_sub", caseless=True
                                  ).setResultsName('function_name')

  _INTERVAL_UNIT = pyp.oneOf(
      "microsecond second minute hour day week month quarter year "
      "second_microsecond minute_microsecond minute_second hour_microsecond "
      "hour_second hour_minute day_microsecond day_second day_minute "
      "day_hour year_month", caseless=True
      ).setResultsName('interval_unit')

  _DATE_FUNCTION = pyp.Group(
      _DATE_FUNCTION_NAME
      + pyp.Suppress('(')
      + _EXPRESSION.setResultsName('arg')
      + pyp.Suppress(',')
      + _INTERVAL_TOKEN
      + _EXPRESSION.setResultsName('interval_val')
      + _INTERVAL_UNIT
      + pyp.Suppress(')')
      ).setResultsName('function')

  _FUNCTION_NAME = (_IDENTIFIER
                    ).setResultsName('function_name')

  _ARG_LIST = pyp.Group(
      pyp.Suppress('(')
      + pyp.Optional(pyp.delimitedList(_EXPRESSION.setResultsName('arg')))
      + pyp.Suppress(')')
      ).setResultsName('args')

  _FUNCTION = pyp.Group(
      _FUNCTION_NAME
      + _ARG_LIST
      ).setResultsName('function')

  _VARIABLE = pyp.Group(
      pyp.Group(pyp.Literal('@@')
                | pyp.Literal('@')
                ).setResultsName('scope')
      + _IDENTIFIER.setResultsName('variable'))

  _LVAL = ((pyp.Suppress('(') + _EXPRESSION + pyp.Suppress(')'))
           | _VAL
           | _FUNCTION
           | _DATE_FUNCTION
           | _COLUMN_NAME + pyp.Optional(
               _COLLATE_TOKEN + _IDENTIFIER.setResultsName('collate'))
           | _VARIABLE)

  _IN_EXPRESSION = pyp.Group(
      _LVAL
      + pyp.Optional(_NOT_TOKEN)
      + _IN_TOKEN
      + pyp.Suppress('(')
      + pyp.delimitedList(_VAL)
      + pyp.Suppress(')')
      ).setResultsName('in')

  _IS_EXPRESSION = pyp.Group(
      _LVAL
      + _IS_TOKEN
      + pyp.Optional(_NOT_TOKEN)
      + (_NULL_TOKEN | _TRUE_TOKEN | _FALSE_TOKEN | _UNKNOWN_TOKEN)
      ).setResultsName('is')

  _CASES_LIST = (
      pyp.OneOrMore(_WHEN_TOKEN
                    + _EXPRESSION
                    + _THEN_TOKEN
                    + _EXPRESSION)
      + pyp.Optional(_ELSE_TOKEN
                     + _EXPRESSION))

  _CASE_EXPRESSION = pyp.Group(
      _CASE_TOKEN
      + (_CASES_LIST
         | (_EXPRESSION + _CASES_LIST))
      + _END_TOKEN).setResultsName('case')

  _UNARY = (
      _NOT_TOKEN
      | '!'
      | '-')

  _EXPRESSION0 = (
      _IS_EXPRESSION
      | _IN_EXPRESSION
      | _CASE_EXPRESSION
      | (pyp.Optional(_UNARY) + _LVAL))

  _EXPRESSION1 = (
      pyp.Group(_EXPRESSION0
                + pyp.ZeroOrMore(_BINOP1 + _EXPRESSION0)).setResultsName('ex'))

  _EXPRESSION2 = (
      pyp.Group(_EXPRESSION1
                + pyp.ZeroOrMore(_BINOP2 + _EXPRESSION1)).setResultsName('ex'))

  _EXPRESSION3 = (
      pyp.Group(_EXPRESSION2
                + pyp.ZeroOrMore(_BINOP3 + _EXPRESSION2)).setResultsName('ex'))
  _EXPRESSION4 = (
      pyp.Group(_EXPRESSION3
                + pyp.ZeroOrMore(
                    pyp.Optional(_NOT_TOKEN) + _BINOP4 + _EXPRESSION3)
                ).setResultsName('ex'))

  _EXPRESSION5 = (
      pyp.Group(_EXPRESSION4
                + pyp.ZeroOrMore(_BINOP5 + _EXPRESSION4)).setResultsName('ex'))

  _EXPRESSION << (
      pyp.Group(_EXPRESSION5
                + pyp.ZeroOrMore(_BINOP6 + _EXPRESSION5)).setResultsName('ex'))

  # SET STATEMENT

  _SET_VARIABLE = (
      pyp.Optional(
          _LOCAL_TOKEN
          | _SESSION_TOKEN
          | _GLOBAL_TOKEN
          | pyp.Literal('@@')
          | pyp.Literal('@')
          ).setResultsName('scope')
      + _IDENTIFIER.setResultsName('variable')
      + pyp.Literal('=')
      + _EXPRESSION)

  _SET_CHARSET = (
      _CHARACTER_TOKEN
      + _SET_TOKEN
      + _EXPRESSION)

  _SET_NAMES = (
      _NAMES_TOKEN
      + _EXPRESSION)

  _SET_SQL = pyp.Group(
      _SET_TOKEN
      + pyp.delimitedList(_SET_VARIABLE
                          | _SET_CHARSET
                          | _SET_NAMES))

  # TABLE REFERENCE

  _INDEX_HINT = ((_USE_TOKEN | _IGNORE_TOKEN | _FORCE_TOKEN)
                 + (_INDEX_TOKEN | _KEY_TOKEN)
                 + pyp.Suppress('(')
                 + pyp.delimitedList(_IDENTIFIER)
                 + pyp.Suppress(')'))

  _ALIAS = (pyp.Optional(_AS_TOKEN)
            + pyp.NotAny(_KEYWORDS)
            + _IDENTIFIER.setResultsName('alias'))

  _TABLE = (pyp.Group(_TABLE_NAME
                      + pyp.Optional(_ALIAS)).setResultsName('table_alias')
            + pyp.Optional(pyp.delimitedList(_INDEX_HINT)))

  _JOIN_CONDITION = ((_ON_TOKEN + _EXPRESSION)
                     | pyp.Group(_USING_TOKEN
                                 + _COLUMN_LIST).setResultsName('using'))

  _JOIN_LEFT_RIGHT = ((_LEFT_TOKEN | _RIGHT_TOKEN)
                      + pyp.Optional(_OUTER_TOKEN))

  _JOIN_SIDE = pyp.Group((_INNER_TOKEN | _CROSS_TOKEN)
                         |(_NATURAL_TOKEN
                           + pyp.Optional(_JOIN_LEFT_RIGHT))
                         | _JOIN_LEFT_RIGHT
                         ).setResultsName('join_side')

  _TABLE_JOIN = pyp.Group(
      pyp.Optional(_JOIN_SIDE)
      + (_JOIN_TOKEN | _STRAIGHT_JOIN_TOKEN)
      + _TABLE
      + pyp.Optional(_JOIN_CONDITION)).setResultsName('tablejoin')

  _TABLE_REFERENCE = _TABLE + pyp.ZeroOrMore(_TABLE_JOIN)
  _TABLE_REFERENCES = pyp.Group(pyp.delimitedList(_TABLE_REFERENCE))


  # DATA MANIPULATION COMMONS

  _EXPRESSION_LIST = pyp.Group(pyp.delimitedList(_EXPRESSION))

  _WHERE = (_WHERE_TOKEN
            + _EXPRESSION_LIST.setResultsName('where'))


  _ORDER_BY = (_ORDER_TOKEN
               + _BY_TOKEN
               + _EXPRESSION_LIST.setResultsName('order_by'))

  _GROUP_BY = (_GROUP_TOKEN
               + _BY_TOKEN
               + _EXPRESSION_LIST.setResultsName('group_by'))

  _HAVING = (_HAVING_TOKEN
             + _EXPRESSION_LIST.setResultsName('having'))

  _LIMIT = (_LIMIT_TOKEN
            + _NUMBER.setResultsName('limit'))

  _SET_VALUE = pyp.Group(_COLUMN_NAME
                         + pyp.Suppress('=')
                         + _EXPRESSION.setResultsName('set_value')
                         ).setResultsName('set')

  _SET_VALUE_LIST = pyp.Group(pyp.delimitedList(_SET_VALUE)
                              ).setResultsName('sets')

  _SET = (_SET_TOKEN.suppress()
          + _SET_VALUE_LIST)

  # SELECT STATEMENTS

  _SELECT_EXPRESSION = (pyp.Group(
      _EXPRESSION.setResultsName('select_expression')
      + pyp.Optional(_AS_TOKEN
                     + _IDENTIFIER.setResultsName('alias')))
                        | pyp.Suppress('*'))

  _SELECT_FROM = pyp.Group(_FROM_TOKEN
                           + _TABLE_REFERENCES).setResultsName('select_from')

  _SELECT_SQL_2 = (_SELECT_FROM
                   + pyp.Optional(_WHERE)
                   + pyp.Optional(_GROUP_BY)
                   + pyp.Optional(_HAVING)
                   + pyp.Optional(_ORDER_BY)
                   + pyp.Optional(_LIMIT))

  _SELECT_OPTIONS = (_ALL_TOKEN
                     | _DISTINCT_TOKEN
                     | _DISTINCTROW_TOKEN)

  _SELECT_SQL = pyp.Group(_SELECT_TOKEN
                          + pyp.Optional(_SELECT_OPTIONS)
                          + pyp.delimitedList(_SELECT_EXPRESSION)
                          .setResultsName('select_expressions')
                          + pyp.Optional(_SELECT_SQL_2)
                          ).setResultsName('select')


  # UPDATE STATEMENTS

  _UPDATE_TABLE = (_TABLE_NAME
                   + _SET
                   + pyp.Optional(_WHERE)
                   + pyp.Optional(_ORDER_BY)
                   + pyp.Optional(_LIMIT))

  _UPDATE_TABLE_REFERENCE = (_TABLE_REFERENCES
                             + _SET
                             + pyp.Optional(_WHERE))

  _UPDATE_SQL = pyp.Group(_UPDATE_TOKEN
                          + (_UPDATE_TABLE
                             | _UPDATE_TABLE_REFERENCE)
                          ).setResultsName('update')
  # INSERT/REPLACE STATEMENTS

  _VALUES = pyp.Group(pyp.Suppress('(')
                      + pyp.delimitedList(_EXPRESSION)
                      + pyp.Suppress(')')
                      ).setResultsName('vals')

  _INSERT_VALUES = (pyp.Optional(_COLUMN_LIST)
                    + _VALUES_TOKEN
                    + pyp.delimitedList(_VALUES))

  _INSERT_SET = _SET

  _INSERT_SELECT = (pyp.Optional(_COLUMN_LIST)
                    + pyp.Optional(pyp.Suppress('('))
                    + pyp.Group(_SELECT_SQL).setResultsName('source_select')
                    + pyp.Optional(pyp.Suppress(')')))

  _ON_DUPLICATE_KEY_UPDATE = (_ON_TOKEN
                              + _DUPLICATE_TOKEN
                              + _KEY_TOKEN
                              + _UPDATE_TOKEN
                              + _SET_VALUE_LIST)

  _INSERT_SQL = pyp.Group(_INSERT_TOKEN
                          + pyp.Optional(_IGNORE_TOKEN)
                          + pyp.Optional(_INTO_TOKEN)
                          + _TABLE_NAME
                          + (_INSERT_VALUES
                             | _INSERT_SET
                             | _INSERT_SELECT)
                          + pyp.Optional(_ON_DUPLICATE_KEY_UPDATE)
                          ).setResultsName('insert')

  _REPLACE_SQL = pyp.Group(_REPLACE_TOKEN
                           + pyp.Optional(_INTO_TOKEN)
                           + _TABLE_NAME
                           + (_INSERT_VALUES
                              | _INSERT_SET
                              | _INSERT_SELECT)
                           ).setResultsName('replace')

  # DELETE STATEMENTS

  # DELETE FROM table WHERE ... [ORDER BY ...] [LIMIT ...]
  # WHERE ... is not optional because sql.par demands its existence
  # in this statement type.
  _DELETE_SIMPLE_SQL = pyp.Group(_DELETE_TOKEN
                                 + _FROM_TOKEN
                                 + _TABLE_NAME
                                 + pyp.Optional(_WHERE)
                                 + pyp.Optional(_ORDER_BY)
                                 + pyp.Optional(_LIMIT)
                                 ).setResultsName('delete')

  # DELETE table FROM table_references [WHERE ...]
  _DELETE_MULTI_SQL = pyp.Group(_DELETE_TOKEN
                                + pyp.delimitedList(_TABLE_NAME
                                                    + pyp.Optional('.*'))
                                + _FROM_TOKEN
                                + _TABLE_REFERENCES.setResultsName('exclude')
                                + (pyp.Group(pyp.Optional(_WHERE))
                                   .setResultsName('exclude'))
                                ).setResultsName('delete')

  # DELETE FROM table USING table_references [WHERE ...]
  _DELETE_MULTI_SQL2 = pyp.Group(_DELETE_TOKEN
                                 + _FROM_TOKEN
                                 + pyp.delimitedList(_TABLE_NAME
                                                     + pyp.Optional('.*'))
                                 + _USING_TOKEN
                                 + _TABLE_REFERENCES.setResultsName('exclude')
                                 + (pyp.Group(pyp.Optional(_WHERE))
                                    .setResultsName('exclude'))
                                 ).setResultsName('delete')

  # TRANSACTIONS
  _START_TRANSACTION_SQL = pyp.Group((_START_TOKEN + _TRANSACTION_TOKEN)
                                     | _BEGIN_TOKEN
                                     ).setResultsName('start_transaction')

  _END_TRANSACTION_SQL = pyp.Group(_COMMIT_TOKEN
                                   | _ROLLBACK_TOKEN
                                   ).setResultsName('end_transaction')

  # UNSUPPORTED QUERIES

  _RENAME_TABLE_SQL = (pyp.CaselessKeyword('rename') +
                       pyp.SkipTo(_LINE_DELIMITER).suppress())

  _TRUNCATE_SQL = (pyp.CaselessKeyword('truncate')
                   + pyp.SkipTo(_LINE_DELIMITER).suppress())

  # VERSIONED COMMENTS
  _STATEMENT = pyp.Forward()
  _VERSIONED_COMMENT = (pyp.Literal('/*!')
                        + pyp.Optional(_NUMBER.setResultsName('min_version'))
                        + _STATEMENT
                        + pyp.Literal('*/'))

  # MAIN

  _STATEMENT << pyp.Group(_ALTER_TABLE_SQL
                          | _ALTER_DATABASE_SQL
                          | _CREATE_TABLE_SQL
                          | _CREATE_TABLE_LIKE_SQL
                          | _DROP_TABLE_SQL
                          | _RENAME_TABLE_SQL
                          | _SELECT_SQL
                          | _UPDATE_SQL
                          | _INSERT_SQL
                          | _REPLACE_SQL
                          | _DELETE_MULTI_SQL
                          | _DELETE_MULTI_SQL2
                          | _DELETE_SIMPLE_SQL
                          | _TRUNCATE_SQL
                          | _START_TRANSACTION_SQL
                          | _END_TRANSACTION_SQL
                          | _CREATE_DATABASE_SQL
                          | _DROP_DATABASE_SQL
                          | _CREATE_INDEX_SQL
                          | _SET_SQL
                          | _VERSIONED_COMMENT
                          ).setResultsName('statement')

  _QUERY = pyp.Group(_STATEMENT
                     + _LINE_DELIMITER).setResultsName('query')
  _QUERY.ignore(_COMMENT_LINE)
  _QUERY.ignore(_COMMENT_BLOCK)


class GoogleSQLParser(SQLParser):
  """Extended grammar for SQL within Google"""

  _GOOGLE_SQL_ON_SHARD = (
      pyp.CaselessKeyword('on')
      + pyp.CaselessKeyword('shard')
      + pyp.Group(pyp.delimitedList(SQLParser._NUMBER)).setResultsName('shard'))

  _GOOGLE_SQL_EXTENSION = pyp.Group(_GOOGLE_SQL_ON_SHARD
                                    ).setResultsName('running_scheme')

  _QUERY = pyp.Group(pyp.Optional(_GOOGLE_SQL_EXTENSION)
                     + SQLParser._STATEMENT
                     + SQLParser._LINE_DELIMITER).setResultsName('query')
  _QUERY.ignore(SQLParser._COMMENT_LINE)
  _QUERY.ignore(SQLParser._COMMENT_BLOCK)
