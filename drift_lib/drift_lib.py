#!/usr/bin/python2.6
#
# Copyright 2007-2011 Google Inc.
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

"""Generate checksums on a database, for online data drift checking.

More information is available here:
http://code.google.com/p/google-mysql-tools/wiki/OnlineDataDrift

Original author: Ben Handy
Later maintainer: Mikey Dickerson
"""

import logging
import re
import time

import drift_policies


def VerifyTableNameFormat(tables):
  """Verify each table in tables is in db_name.table_name format.

  Args:
     tables: tuple of table names, or a single table name string.

  Returns:
    The tables if all table in it is in the db.table format.

  Raises:
    Error: If any table is not in db_name.table_name format.
  """
  if not tables:
    return tables
  if isinstance(tables, str):
    temp_tables = (tables,)
  else:
    temp_tables = tables
  for table in temp_tables:
    if not re.match('(\w+)\.(\w+)', table):
      raise Error('Given table %s is not in db.table format.' % table)
  return tables


class Error(Exception):
  """Default Exception class."""
  pass


class AdminTablesMissing(Error):
  """Exception for missing Admin Tables."""

  def __init__(self, message):
    """Constructor for AdminTablesmissing."""

    Error.__init__(self, 'Admin Tables Missing: %s.' % message)


class DbChecksummer(object):
  """Submits checksum queries against a database.

  A DbChecksummer performs a checksumming job that submits many consecutive
  queries against a database, until all desired tables have been checksummed.
  An instance can be re-used, by calling ChecksumTables() repeatedly.

  The DbChecksummer is designed to be used as follows:
  db_checksummer.PrepareToChecksum()
  while db_checksummer.ChecksumQuery():
    time.sleep(db_checksummer.GetNextWait())
  """

  def __init__(self, dbh, result_table, golden_table, job_started=None,
               scan_rate=10000000, secs_per_query=1.0, rows_per_query=None,
               hours_to_run=None, utilization=0.02, tables_to_skip=(),
               engines_to_check=('InnoDB',), tables_to_check=(),
               column_types_to_skip=(),
               databases_to_skip=('information_schema',),
               database_to_check=None):
    """Initialize the DbChecksummer.

    Args:
      dbh: Database connection handle.
      result_table: Table name to write statement-based results to (possibly
        different on master/slave).
      golden_table: Table name to write golden results to (authoritative from
        master).
      job_started: MySQL-formatted date of job start.
      scan_rate: Estimated checksum speed in input bytes per person.
      secs_per_query: Seconds per query (used to auto-scale rows_per_query).
      rows_per_query: Suggested rows per checksum query (overrides
        secs_per_query).
      hours_to_run: Hours to run until completion of a full checksum pass
        (overrides utilization).
      utilization: Fraction of time to query db.
      tables_to_skip: Tuple of tables to skip.
      engines_to_check: Tuple of storage engines to check.
      tables_to_check: Tuple of tables to check.
      column_types_to_skip: Tuple of column types to skip.
      databases_to_skip: Tuple of database name to skip.
      database_to_check: Specify the only database need to be checksummed.

    Raises:
      Error: An error occurred if required parameters such as rows_per_query,
             secs_per_query, and hours_to_run are missing.
    """
    self._dbh = dbh
    self._result_table = VerifyTableNameFormat(result_table)
    self._golden_table = VerifyTableNameFormat(golden_table)
    self._job_started = (job_started or
                         self._dbh.ExecuteOrDie('SELECT NOW()')[0]['NOW()'])
    self._tables = []
    self._current_table = None
    self._monitor = drift_policies.ProgressMonitor(scan_rate)

    if rows_per_query:
      self._batch_sizer = drift_policies.FixedQuerySizePolicy(
          self._monitor, rows_per_query)
    elif secs_per_query:
      self._batch_sizer = drift_policies.FixedQueryTimePolicy(
          self._monitor, secs_per_query)
    else:
      raise Error('Must pass secs_per_query or rows_per_query')

    if hours_to_run:
      self._wait_timer = drift_policies.FixedRunTimePolicy(
          self._monitor, hours_to_run * 3600)
    elif utilization:
      self._wait_timer = drift_policies.FixedUtilizationPolicy(
          self._monitor, utilization)
    else:
      raise Error('Must pass utilization or hours_to_run')

    self._skip_tables = VerifyTableNameFormat(tables_to_skip)
    self._engine_list = engines_to_check
    self._table_list = VerifyTableNameFormat(tables_to_check)
    self._column_types_to_skip = column_types_to_skip
    self._databases_to_skip = databases_to_skip
    self._database_to_check = database_to_check

  def SetDatabaseHandle(self, dbh):
    """After unpickling, we need to set the database connection handle.

    Args:
      dbh: Instance of db.py:BaseConnection
    """
    self._dbh = dbh

  def SetUtilization(self, new_utilization_rate):
    """Change the utilization of the checksum job, even mid-run.

    Args:
      new_utilization_rate: ratio of time to spend checksumming.
    """
    self._wait_timer.SetUtilization(new_utilization_rate)

  def AddFilter(self, sql, filter_columns, filter_values, operator):
    """Append an IN or NOT IN clause to the sql statement.

    Args:
      sql: A base sql statement.
      filter_columns: The table columns need to add filter.
      filter_values: Filter vlaues in a tuple for filter_columns.
      operator: Either 'IN' or 'NOT IN'.

    Returns:
      The original sql or sql with more filters appended to it.
    """

    if not filter_values or len(filter_values) < 1:
      return sql
    filter_values = '(%s) ' % ','.join("'" + p + "'" for p in filter_values)
    sql = '%s AND %s %s %s' % (sql, filter_columns, operator, filter_values)
    return sql

  def _AnalyzeTables(self):
    """Retrieve db schema information to plan/schedule checksumming job.

    After getting the list of tables for checksumming, a monitor is updated
    with the data size estimates so that it can make informed scheduling
    decisions.
    """
    # Get list of tables which need to be checksummed cross multiple databases.
    sql_get_tables = ('SELECT table_schema, table_name, engine, table_rows, '
                      'data_length FROM information_schema.tables '
                      'WHERE lower(table_schema) != \'admin\'')
    sql_get_tables = self.AddFilter(sql_get_tables, 'table_schema.table_name',
                                    self._table_list, 'IN')
    sql_get_tables = self.AddFilter(sql_get_tables, 'table_schema.table_name',
                                    self._skip_tables, 'NOT IN')
    sql_get_tables = self.AddFilter(sql_get_tables, 'table_schema',
                                    self._databases_to_skip, 'NOT IN')
    sql_get_tables = self.AddFilter(sql_get_tables, 'engine',
                                    self._engine_list, 'IN')
    if self._database_to_check:
      sql_get_tables = '%s AND table_schema=\'%s\'' % (sql_get_tables,
                                                       self._database_to_check)
    table_stats = self._dbh.ExecuteOrDie(sql_get_tables)

    self._tables = []
    for row in table_stats:
      self._tables.append(
          TableChecksummer(self._dbh, self._monitor,
                           self._batch_sizer,
                           row['table_schema'],
                           row['table_name'],
                           self._job_started,
                           self._result_table,
                           self._golden_table,
                           column_types_to_skip=self._column_types_to_skip))

      self._monitor.AddTable('%s.%s' % (row['table_schema'], row['table_name']),
                             row['table_rows'], row['data_length'])

  def GetJobStartTime(self):
    """Provides access to the last checksum job start time.

    Returns:
      DD/MM/YYYY HH:MM:SS
    """
    return self._job_started

  def PrepareToChecksum(self):
    """Call once before starting to call ChecksumQuery repeatedly.

    This function also must be called after unpickling, before making any
    checksum queries.
    """
    self._AnalyzeTables()
    self._position = 0
    if self._tables and not self._current_table:
      self._current_table = self._tables[self._position]
    while self._position < len(self._tables):
      if (self._tables[self._position].table_name ==
          self._current_table.table_name):
        # If this is a restore, we use the saved table in _current_table.
        self._tables[self._position] = self._current_table
        self._current_table.SetDatabaseHandle(self._dbh)
        self._current_table.PrepareToChecksum()
        break
      self._position += 1

  def ChecksumQuery(self):
    """Call repeatedly until it returns False to checksum the database.

    Caller should sleep for the number of seconds returned by GetNextWait()
    before issuing the next ChecksumQuery() call.

    Returns:
      False if checksumming is complete, True if there are more queries to run.
    """
    if self._position >= len(self._tables):
      self._current_table = None
      return False
    if not self._tables[self._position].ChecksumQuery():
      self._position += 1
      if self._position < len(self._tables):
        self._tables[self._position].PrepareToChecksum()
        self._current_table = self._tables[self._position]
    return True

  def GetNextWait(self):
    """Suggests time to sleep before next checksum query."""
    return self._wait_timer.GetNextWait()

  def ReportPerformance(self):
    """Provides information about checksumming progress and performance.

    Returns:
      A string containing checksum performance and progress details.
    """
    return self._monitor.ReportPerformance()

  def ChecksumTables(self):
    """Generate checksums for each table.

    This is the public method that executes an entire checksumming job. It
    starts by analyzing the schema, and doesn't return until the checksumming
    job is complete.

    Returns:
      True if ChecksumTables finish successful.

    Raises:
      Exception while if ChecksumTables run into error.
    """
    try:
      self.PrepareToChecksum()
      while self.ChecksumQuery():
        time.sleep(self.GetNextWait())
      return True
    except Exception, e:
      logging.exception(e)
      raise


class TableChecksummer(object):
  """Class responsible for checksumming a single table.

  Members:
  table_name is a publicly accessible string containing the table's name.
  """

  def __init__(self, dbh, monitor, batch_sizer, database, table, job_started,
               result_table, golden_table, column_types_to_skip=(),
               row_condition=''):
    """Initialize TableChecksummer.

    Args:
      dbh: Database connection handle.
      monitor: instance of ProgressMonitor class, tracks progress.
      batch_sizer: subclass of BatchSizePolicy, determines rows per query.
      database: string containing the database name.
      table: string containing the table name.
      job_started: string indicating job start time in MySQL now() format.
      result_table: Table to write statement-generated checksums to.
      golden_table: Table to write literal checksums to.
      column_types_to_skip: List of column tables to not checksum.
      row_condition: SQL expression to choose which rows to checksum.
    """
    self._dbh = dbh
    self._monitor = monitor
    self._batch_sizer = batch_sizer
    self.db_name = database
    self.table_name = table
    self._skip_column_types = column_types_to_skip
    self._row_condition = row_condition
    self._result_table = result_table
    self._golden_table = golden_table
    self._job_started = job_started
    self._chunk = 1
    self._query_dict = {}

  def SetDatabaseHandle(self, dbh):
    """After unpickling, we need to set the database connection handle.

    Args:
      dbh: An instance of db.py:BaseConnection.
    """
    self._dbh = dbh

  def PrepareToChecksum(self):
    """Call before making any calls to ChecksumQuery."""
    if not self._InitQueryDict():
      return
    # If we need to start in the middle of the table, we get the starting
    # offsets from _query_dict['offset_copy'] (last query re-inserted).
    prev_offsets = self._query_dict.get('offset_copy', '')
    offset_array = prev_offsets.split(':')  # Key1:RangeStart1:RangeEnd1:Key2..
    keys = []
    for i in range(len(offset_array) - 2):
      if not i % 3:
        key_field = offset_array[i]
        key_value = offset_array[i+2]
        keys.append("%s = '%s'" % (key_field, key_value.replace("'", "\\'")))
    if keys:
      self._query_dict['initial_where'] = 'WHERE %s' % ' AND '.join(keys)
    try:
      self._dbh.ExecuteOrDie(
          TableChecksummer.INITIALIZATION_QUERY % self._query_dict)
    except Exception, e:
      logging.exception(e)
      raise AdminTablesMissing(str(e))

  def ChecksumQuery(self):
    """Issues one checksum query against the table.

    Should be called repeatedly until it returns False.
    Caller should call GetNextWait to determine sleep time before next query

    Returns:
      False when the table has completed checksumming, True otherwise.

    Raises:
      AdminTableMissing: If drift table doesn't exist.
    """
    rows_to_read = self._batch_sizer.GetNextBatchSize('%s.%s'
                                                      %(self.db_name,
                                                        self.table_name))
    self._query_dict['batch_size'] = rows_to_read
    self._query_dict['chunk'] = self._chunk
    self._chunk += 1
    start_time = time.time()
    self._dbh.ExecuteOrDie(
        'SELECT %(subsequent_assignment)s' % self._query_dict)
    try:
      self._dbh.ExecuteOrDie(
          TableChecksummer.CHECKSUM_QUERY % self._query_dict)
    except Exception, e:
      logging.exception(e)
      raise AdminTablesMissing(str(e))

    count_result = self._dbh.ExecuteOrDie('SELECT @count')
    if not count_result:  # If we encounter a problem, give up on this table
      return False
    rows_read = count_result[0]['@count']
    if rows_read is None:
      return False
    self._monitor.RecordProgress(rows_read, time.time() - start_time,
                                 '%s.%s' %(self.db_name, self.table_name))
    self._monitor.ReportPerformance()
    self._ReinsertByValue()
    return rows_read == rows_to_read

  def _ReinsertByValue(self):
    """Selects a checksum entry, submits a new insert with the same values."""
    # Returns ChunkDone, Offsets, Checksums, Count
    rows = self._dbh.ExecuteOrDie(
        TableChecksummer.GET_CHECKSUM_QUERY % self._query_dict)
    # TODO(benhandy): how should we handle this error?
    if not rows or len(rows) != 1:
      return
    row = rows[0]

    self._query_dict['chunk_done_copy'] = row['ChunkDone']
    self._query_dict['offset_copy'] = row['Offsets']
    self._query_dict['checksum_copy'] = row['Checksums']
    self._query_dict['count_copy'] = row['Count']
    self._dbh.ExecuteOrDie(
        TableChecksummer.INSERT_CHECKSUM_QUERY % self._query_dict)

  def _IdentifyColumns(self):
    """Identify primary key and other columns, applying column filters.

    Returns:
      True if the columns and primary key were found, False otherwise.
    """

    sql_columns = ('SELECT column_name Field, data_type Type '
                   'FROM information_schema.columns '
                   'WHERE table_schema = "%s" AND table_name ="%s"')
    rows = self._dbh.ExecuteOrDie(sql_columns % (self.db_name, self.table_name))
    if not rows:  # First checksum query will fail
      return False

    self._primary_key = []
    self._columns = []
    for row in rows:
      datatype = row['Type']
      if '(' in datatype:
        datatype = datatype[:datatype.index('(')]  # bigint(20) -> bigint
      if datatype not in self._skip_column_types:
        self._columns.append(row['Field'])
    sql_indexes = ('SELECT index_name Key_name, column_name Column_name '
                   'FROM information_schema.statistics '
                   'WHERE table_schema = "%s" AND table_name = "%s"')
    rows = self._dbh.ExecuteOrDie(sql_indexes % (self.db_name, self.table_name))
    if not rows:
      return False
    for row in rows:
      if row['Key_name'] == 'PRIMARY':
        self._primary_key.append(row['Column_name'])

    # If there is no primary key, we will not attempt to checksum the table.
    if not self._primary_key:
      return False
    return True

  def _InitQueryDict(self):
    """Generate table-specific re-usable portions of the query.

    Returns:
      True if the query dictionary is correctly initialized, False otherwise.
    """
    if not self._IdentifyColumns():
      return False
    offset_str = ', '.join(["'%s', @start_%s, @next_%s := last_value(%s)" %
                            (key, key, key, key) for key in self._primary_key])
    offset_str = "concat_ws(':', %s)" % offset_str

    checksum_str = ', '.join(["'%s\', ORDERED_CHECKSUM(%s)" % (col, col)
                              for col in self._columns])
    checksum_str = "concat_ws(':', %s)" % checksum_str
    primary_key_str = ', '.join(key for key in self._primary_key)
    initial_assign_list = ['@next_%s := %s' % (key, key)
                           for key in self._primary_key]
    initial_assign_str = ', '.join(initial_assign_list)
    subsequent_assign_str = ', '.join('@start_%s := @next_%s' %
                                      (key, key) for key in self._primary_key)

    # Tricky python to produce where clause: (p1=c1 and p2>c2) or p1>c1
    and_clauses = []
    for i in range(len(self._primary_key)):
      # This iteration adds and-clause with greater-than on column i
      and_terms = ['%s = @start_%s' % (key, key)
                   for key in self._primary_key[:i]]
      and_terms.append('%s > @start_%s' % (self._primary_key[i],
                                           self._primary_key[i]))
      and_clauses.append('(' + ' and '.join(and_terms) + ')')
    where_str = ' OR '.join(and_clauses)
    if not where_str:
      where_str = 'TRUE'
    if self._row_condition:
      where_str = ' AND '.join(['(' + where_str + ')', self._row_condition])

    self._query_dict.update({'table': self.table_name,
                             'db_name': self.db_name,
                             'result_table': self._result_table,
                             'golden_table': self._golden_table,
                             'offsets': offset_str,
                             'checksums': checksum_str,
                             'where': where_str,
                             'primary_key': primary_key_str,
                             'initial_assignment': initial_assign_str,
                             'subsequent_assignment': subsequent_assign_str,
                             'initial_where': '',  # Only for resuming runs
                             'job_started': self._job_started,
                             'chunk': 1,        # Set for each query
                             'batch_size': 1})  # Set for each query
    return True

# SQL Queries
  INITIALIZATION_QUERY = """
SELECT %(initial_assignment)s
FROM %(db_name)s.%(table)s
%(initial_where)s
ORDER BY %(primary_key)s
LIMIT 1
""".replace('\n', ' ').strip()

  CHECKSUM_QUERY = """
REPLACE INTO %(result_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
SELECT '%(db_name)s', '%(table)s', %(chunk)s, '%(job_started)s',
NOW(), %(offsets)s, %(checksums)s, @count := count(*)
FROM (SELECT * FROM %(table)s FORCE INDEX (PRIMARY)
WHERE %(where)s
ORDER BY %(primary_key)s
LIMIT %(batch_size)s) f
""".replace('\n', ' ').strip()

  GET_CHECKSUM_QUERY = """
SELECT ChunkDone, Offsets, Checksums, Count
FROM %(result_table)s
WHERE DatabaseName='%(db_name)s' AND
TableName='%(table)s' AND
Chunk=%(chunk)s AND
JobStarted='%(job_started)s' AND
Count=@count
""".replace('\n', ' ').strip()

  INSERT_CHECKSUM_QUERY = """
REPLACE INTO %(golden_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
VALUES ('%(db_name)%', '%(table)s', %(chunk)s, '%(job_started)s',
'%(chunk_done_copy)s', '%(offset_copy)s', '%(checksum_copy)s',
%(count_copy)s)
""".replace('\n', ' ').strip()


class DbChecksumVerifier(object):
  """Class for verifying data drift checksums result on a replica database.

  The DbChecksumVerifier performs a checksum verification to report if any data
  drift happen between master and slave.

  It is designed to be used as follows:
  dbchecksum_verifier =  DbChecksumVerifier(dbh, 'admin.Checksums',
                                           'admin.ChecksumGolden')
  boolean_result = dbchecksum_verifier.VerifyChecksums()
  """

  def __init__(self, dbh, result_table='admin.Checksums',
               golden_table='admin.ChecksumsGolden'):
    """Constructor.

    Args:
      dbh: Database connection handle.
      result_table: Table name to store the checksum result on slave,
                    such as admin.Checksums
      golden_table: Table name to store the checksum result from master,
                    such as admin.ChecksumsGolden
    """
    self._dbh = dbh
    self._result_table = VerifyTableNameFormat(result_table)
    self._golden_table = VerifyTableNameFormat(golden_table)
    self._param_dict = {}
    self._param_dict['result_table'] = self._result_table
    self._param_dict['golden_table'] = self._golden_table

  def VerifyChecksums(self, slave_check=True):
    """Verifies datadrift checksums are correct and deletes verified values.

    Args:
      slave_check: If True, will pre-verify we're running on a slave and not
                   on a master.
    Returns:
      True: If no data drift detected.
      False: If data drift detected or other abnormal situation.

    Raises:
      AdminTableMissing: If drift table doesn't exist.
    """
    if slave_check:
      if not self._dbh.ExecuteOrDie('SHOW SLAVE STATUS'):
        logging.warning('DbChecksumVerifier is not running '
                        'since this server is the master.')
        return False

    # Query to see latest computed checksum
    try:
      date_results = self._dbh.ExecuteOrDie(self.CHECKSUM_DATE_QUERY %
                                            self._param_dict)
      if not date_results:
        logging.warning('No checksums data computed.')
        return False
      # Delete matching checksums
      self._dbh.ExecuteOrDie(self.DELETE_CHECKSUMS_QUERY % self._param_dict)

      # Query for remaining checksums
      checksummed_rows = self._dbh.ExecuteOrDie(self.CHECKSUM_VERIFY_QUERY %
                                                self._param_dict)
    except Exception, e:
      logging.exception(e)
      raise AdminTablesMissing(str(e))

    if not checksummed_rows:
      return True

    # Each returned row indicates a problem. Gather details on each problem.
    errors = []
    for row in checksummed_rows:
      # Ensure boundary key values match
      if self.CompareOffsets(row, errors):
        continue
      # Ensure checksum results match
      self.CompareChecksums(row, errors)

    error_string = ', '.join(errors)[0:1024]
    logging.error('Data Drift happened: %s.', error_string)
    return False

  def QueryName(self, row):
    """Returns a description of the checksum query from a verification row."""
    return '%s %s.%s.%s' % (row['JobStarted'], row['DatabaseName'],
                            row['TableName'], row['Chunk'])

  def CompareOffsets(self, row, errors):
    """Look for a mismatch of the primary key offsets in a verification row."""
    if row['LocalOffsets'] != row['GoldenOffsets']:
      errors.append('OffsetMismatch: %s [(%s), (%s)]' % (self.QueryName(row),
                                                         row['LocalOffsets'],
                                                         row['GoldenOffsets']))
      return True
    elif row['LocalCount'] != row['GoldenCount']:
      errors.append('RowCountMismatch: %s [%s, %s]' % (self.QueryName(row),
                                                       row['LocalCount'],
                                                       row['GoldenCount']))
      return True
    return False

  def CompareChecksums(self, row, errors):
    """Look for a checksum mismatch in a verification row."""
    if row['LocalChecksums'] == row['GoldenChecksums']:
      return False
    local_checksum_list = row['LocalChecksums'].split(':')
    golden_checksum_list = row['GoldenChecksums'].split(':')
    # Checksum fields are invalid with odd lengths or different lengths
    if len(local_checksum_list) % 2:
      errors.append('MissingLocalChecksum: %s [(%s), (%s)]' % (
          self.QueryName(row), row['LocalChecksums'], row['GoldenChecksums']))
      return True
    elif len(golden_checksum_list) % 2:
      errors.append('MissingGoldenChecksum: %s [(%s), (%s)]' % (
          self.QueryName(row), row['LocalChecksums'], row['GoldenChecksums']))
      return True
    elif len(local_checksum_list) != len(golden_checksum_list):
      errors.append('ColumnCountMismatch: %s [%s, %s]' % (
          self.QueryName(row), len(local_checksum_list) / 2,
          len(golden_checksum_list) / 2))
      return True
    # Iterate over the list of column names and checksum results
    for i in range(len(local_checksum_list)):
      if i % 2:  # Entries alternate between column name and checksum result
        continue
      if local_checksum_list[i] != golden_checksum_list[i]:
        errors.append('ColumnNameMismatch: %s [%s, %s]' % (
            self.QueryName(row), local_checksum_list[i],
            golden_checksum_list[i]))
      elif local_checksum_list[i+1] != golden_checksum_list[i+1]:
        errors.append('ColumnChecksumMismatch: %s.%s [%s, %s]' % (
            self.QueryName(row), local_checksum_list[i],
            local_checksum_list[i+1], golden_checksum_list[i+1]))

  CHECKSUM_VERIFY_QUERY = """
SELECT
Local.DatabaseName AS DatabaseName,
Local.TableName AS TableName,
Local.Chunk AS Chunk,
Local.JobStarted AS JobStarted,
Local.ChunkDone AS LocalChunkDone,
Local.Offsets AS LocalOffsets,
Local.Checksums AS LocalChecksums,
Local.Count AS LocalCount,
Golden.ChunkDone AS GoldenChunkDone,
Golden.Offsets AS GoldenOffsets,
Golden.Checksums AS GoldenChecksums,
Golden.Count AS GoldenCount
FROM %(result_table)s AS Local
JOIN %(golden_table)s AS Golden
USING (DatabaseName, TableName, JobStarted, Chunk)
WHERE (Local.Offsets != Golden.Offsets OR
Local.Checksums != Golden.Checksums OR
Local.Count != Golden.Count)
""".replace('\n', ' ').strip()

  DELETE_CHECKSUMS_QUERY = """
DELETE %(result_table)s, %(golden_table)s
FROM %(result_table)s AS Local, %(golden_table)s AS Golden
WHERE Local.DatabaseName = Golden.DatabaseName AND
Local.TableName = Golden.TableName AND
Local.JobStarted = Golden.JobStarted AND
Local.Chunk = Golden.Chunk AND
Local.Offsets = Golden.Offsets AND
Local.Checksums = Golden.Checksums AND
Local.Count = Golden.Count
""".replace('\n', ' ').strip()

  CHECKSUM_DATE_QUERY = """
SELECT JobStarted, ChunkDone
FROM %(result_table)s
ORDER BY ChunkDone DESC LIMIT 1
""".replace('\n', ' ').strip()
