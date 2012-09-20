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
Later maintainer: Mikey Dickerson, Michael Kacirek
"""

import logging
import re
import time

import drift_policies

try:
  from pylib import range_lib
except ImportError:
  from ..pylib import range_lib

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
  db_checksummer = DbChecksummer(...)
  db_checksummer.ChecksumTables()
  """

  def __init__(self, dbh, result_table, golden_table, log_table,
               job_started=None, scan_rate=10000000, secs_per_query=1.0,
               rows_per_query=None, hours_to_run=None, utilization=0.02,
               tables_to_skip=(), engines_to_check=('InnoDB',),
               tables_to_check=(), column_types_to_skip=(),
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
    self._log_table = VerifyTableNameFormat(log_table)
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

  def _SetDatabaseHandle(self, dbh):
    """After unpickling, we need to set the database connection handle.

    Args:
      dbh: Instance of db.py:BaseConnection
    """
    self._dbh = dbh

  def _SetUtilization(self, new_utilization_rate):
    """Change the utilization of the checksum job, even mid-run.

    Args:
      new_utilization_rate: ratio of time to spend checksumming.
    """
    self._wait_timer.SetUtilization(new_utilization_rate)

  def _AddFilter(self, sql, filter_columns, filter_values, operator):
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

  def _LogChecksumCompletion(self, table_schema, table_name):
    """Update the admin.ChecksumsComplete table to reflect  checksum complete.

    Args:
      table_name (str): The name of the table that has completed.
    Returns:
      None
    """
    sql = """REPLACE INTO %(log_table)s
      (DatabaseName, TableName, LastChecksumTime)
      VALUES (%(db_name)s, %(table_name)s, NOW())
    """
    params = {'log_table': ('literal',self._log_table),
              'db_name': table_schema,
              'table_name': table_name}
    self._dbh.ExecuteOrDie(sql, params)


  def _AnalyzeTables(self):
    """Retrieve db schema information to plan/schedule checksumming job.

    After getting the list of tables for checksumming, a monitor is updated
    with the data size estimates so that it can make informed scheduling
    decisions.
    """
    # Get list of tables which need to be checksummed cross multiple databases.
    # Filtering out tables without primary key in this level to avoid processing
    # them further.
    sql_get_tables = (
        'SELECT DISTINCT '
        't.table_schema, '
        't.table_name, '
        't.engine, '
        't.table_rows, '
        't.data_length '
        'FROM '
        'information_schema.tables t '
        'JOIN information_schema.statistics s '
        'ON t.table_schema = s.table_schema '
        'AND '
        't.table_name = s.table_name '
        'WHERE '
        'lower(t.table_schema) not in (\'admin\') '
        'AND '
        's.index_name = \'PRIMARY\'')
    sql_get_tables = self._AddFilter(
        sql_get_tables,
        'CONCAT(t.table_schema, \'.\' , t.table_name)',
        self._table_list,
        'IN')
    sql_get_tables = self._AddFilter(
        sql_get_tables,
        'CONCAT(t.table_schema, \'.\', t.table_name)',
        self._skip_tables,
        'NOT IN')
    sql_get_tables = self._AddFilter(
        sql_get_tables,
        't.table_schema',
        self._databases_to_skip,
        'NOT IN')
    sql_get_tables = self._AddFilter(
        sql_get_tables,
        't.engine',
        self._engine_list,
        'IN')
    if self._database_to_check:
      sql_get_tables = '%s AND t.table_schema=\'%s\'' % (
          sql_get_tables, self._database_to_check)
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

  def _PrepareToChecksumDb(self):
    """Call once before starting to call _DoDbChecksum repeatedly.

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
        self._current_table._SetDatabaseHandle(self._dbh)
        self._current_table._PrepareToChecksumTable()
        break
      self._position += 1

  def _DoDbChecksum(self):
    """Call repeatedly until it returns False to checksum the database.

    Caller should sleep for the number of seconds returned by GetNextWait()
    before issuing the next _DoDbChecksum() call.

    Returns:
      False if checksumming is complete, True if there are more queries to run.
    """
    if self._position >= len(self._tables):
      self._current_table = None
      logging.info('No more db tables to checksum.')
      return False
    if not self._tables[self._position]._DoTableChecksum():
      logging.info('TableChecksum for: %s returned false, continuing',
                   self._tables[self._position])
      # Table has completed checksum, log completion.
      db_name, table_name = self._tables[self._position].GetTableName()
      try:
        self._LogChecksumCompletion(db_name, table_name)
      except Exception as e:
        logging.exception(e)
        raise AdminTablesMissing(str(e))
      self._position += 1
      while (self._position < len(self._tables) and not
             self._tables[self._position]._PrepareToChecksumTable()):
        self._position += 1
        logging.info('self.position: %s', self._position)
      # Make sure table ready to be checksummed, not out of position.
      if self._position < len(self._tables):
        self._current_table = self._tables[self._position]
    return True

  def _GetNextWait(self):
    """Suggests time to sleep before next checksum query."""
    return self._wait_timer.GetNextWait()

  def _ReportPerformance(self):
    """Provides information about checksumming progress and performance.

    Returns:
      A string containing checksum performance and progress details.
    """
    return self._monitor.ReportPerformance()

  def ChecksumTables(self):
    """Generate checksums for each table.

    This is the public method that executes an entire checksumming job. It
    starts by analyzing the schema and doesn't return until the checksumming
    job is complete.

    Returns:
      True if finished successful.

    Raises:
      Exception while if ChecksumTables run into error.
    """
    try:
      self._PrepareToChecksumDb()
      while self._DoDbChecksum():
        time.sleep(self._GetNextWait())
        logging.info(self._ReportPerformance())
      return True
    except Exception as e:
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
    self._result_table = result_table
    self._golden_table = golden_table
    self._job_started = job_started
    self._chunk = 1
    self._from_key = None
    self._query_dict = {}

  def _InitQueryDict(self):
    if not self._IdentifyColumns():
      logging.info('Failed to initialize Table Columns.')
      return False
    self._query_dict['db_name'] = self.db_name
    self._query_dict['table_name'] = self.table_name
    self._query_dict['result_table'] = self._result_table
    self._query_dict['golden_table'] = self._golden_table
    self._query_dict['job_started'] = self._job_started
    self._query_dict['checksums'] = self._GetChecksumSelectExpr()

  def _GetChecksumSelectExpr(self):
    """Return the SELECT column list portion of the query string.

    This should return a string of the form:
    CONCAT_WS(':', colA, ORDERED_CHECKSUM(colA), ...)

    Args:
      columns: the list of columns in this table
    """
    col_list = ['":"']
    for col in self._columns:
      col_list.append('"%s"' % col)
      col_list.append('ORDERED_CHECKSUM(%s)' %(col))

    return 'CONCAT_WS(%s)' %(','.join(col_list))

  def _GetOffsetsSelectExpr(self, from_key, to_key):
    col_list = []
    for col in self._primary_key:
      col_list.append(col)
      col_list.append(from_key.get(col))
      col_list.append(to_key.get(col))

    return ':'.join(col_list)

  def _SetDatabaseHandle(self, dbh):
    """After unpickling, we need to set the database connection handle.

    Args:
      dbh: An instance of db.py:BaseConnection.
    """
    self._dbh = dbh

  def _PrepareToChecksumTable(self):
    """Call before making any calls to _DoTableChecksum.

    Returns:
      True: If table is prepared and ready for checksum.
      False: Id table is not able to be checksummed.
    """
    self._InitQueryDict()
    logging.info('Preparing: %s.%s', self.db_name, self.table_name)
    # Identify columns (get primary keys, etc).
    self._range = range_lib.PrimaryKeyRange(self._dbh, self.db_name,
                                            self.table_name)

    if not self._from_key:
      self._from_key = self._range.GetFirstPrimaryKeyValue()
    return True

  def _GenerateChecksumQuery(self, from_key, to_key):
    self._query_dict['chunk'] = self._chunk
    self._query_dict['offsets'] = self._GetOffsetsSelectExpr(from_key, to_key)
    self._query_dict['started'] = time.time()
    self._query_dict['where'] = self._range.GenerateRangeWhere(from_key, to_key)
    return self.CHECKSUM_QUERY % self._query_dict

  def _DoTableChecksum(self):
    """Issues one checksum query against the table.

    Should be called repeatedly until it returns False.
    Caller should call _GetNextWait to determine sleep time before next query

    Returns:
      False when the table has completed checksumming, True otherwise.

    Raises:
      AdminTableMissing: If drift table doesn't exist.
    """
    from_key = self._from_key
    if not from_key:
      return False
    batch_size = self._batch_sizer.GetNextBatchSize('%s.%s'
                                                    %(self.db_name,
                                                      self.table_name))
    logging.info('Processing table: %s.%s bs: %s chunk: %s',
      self.db_name, self.table_name, batch_size, self._chunk)
    to_key = self._range.GetNthPrimaryKeyValue(batch_size, from_key)
    cs_query = self._GenerateChecksumQuery(from_key, to_key)
    start_time = time.time()
    try:
      self._dbh.ExecuteOrDie(cs_query)
      count = self._dbh.ExecuteOrDie('SELECT @count')
    except Exception as e:
      logging.exception(e)
    if not count:
      return False
    rows_read = count[0]['@count']
    if not rows_read:
      return False
    self._monitor.RecordProgress(rows_read, time.time() - start_time,
      '%s.%s' %(self.db_name, self.table_name))
    self._monitor.ReportPerformance()
    self._ReinsertByValue()
    self._chunk += 1
    self._from_key = to_key
    # Completion conditions
    if from_key == to_key or (batch_size + 1) != rows_read:
      logging.info('%s == %s or %s != %s',
        from_key, to_key, batch_size + 1, rows_read)
      return False
    return True

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

  def GetTableName(self):
    return (self.db_name, self.table_name)

  def __str__(self):
    return '<table checksummer for: %s.%s>' %(self.db_name, self.table_name)

  CHECKSUM_QUERY = """
REPLACE INTO %(result_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
SELECT '%(db_name)s', '%(table_name)s', %(chunk)s, '%(job_started)s',
NOW(), '%(offsets)s', %(checksums)s, @count := count(*)
FROM %(db_name)s.%(table_name)s FORCE INDEX (PRIMARY)
WHERE %(where)s
"""

  GET_CHECKSUM_QUERY = """
SELECT ChunkDone, Offsets, Checksums, Count
FROM %(result_table)s
WHERE DatabaseName='%(db_name)s' AND
TableName='%(table_name)s' AND
Chunk=%(chunk)s AND
JobStarted='%(job_started)s' AND
Count=@count
"""

  INSERT_CHECKSUM_QUERY = """
REPLACE INTO %(golden_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
VALUES ('%(db_name)s', '%(table_name)s', %(chunk)s, '%(job_started)s',
'%(chunk_done_copy)s', '%(offset_copy)s', '%(checksum_copy)s',
%(count_copy)s)
"""

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
    except Exception as e:
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
"""

  DELETE_CHECKSUMS_QUERY = """
DELETE %(result_table)s, %(golden_table)s
FROM %(result_table)s
JOIN %(golden_table)s
USING (DatabaseName, TableName, JobStarted, Chunk, Offsets, Checksums, Count)
"""

  CHECKSUM_DATE_QUERY = """
SELECT JobStarted, ChunkDone
FROM %(result_table)s
ORDER BY ChunkDone DESC LIMIT 1
"""
